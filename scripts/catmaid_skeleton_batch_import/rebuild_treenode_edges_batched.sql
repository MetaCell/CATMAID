CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_treenode_edge_rebuild_state (
    project_id integer PRIMARY KEY,
    batch_size integer NOT NULL,
    target_rows bigint,
    status text NOT NULL DEFAULT 'running',
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    last_error text
);

CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_treenode_edge_rebuild_progress (
    project_id integer NOT NULL,
    batch_no bigint NOT NULL,
    id_min bigint NOT NULL,
    id_max bigint NOT NULL,
    rows_selected bigint NOT NULL,
    rows_deleted bigint,
    rows_inserted_nonroot bigint,
    rows_inserted_root bigint,
    wal_start pg_lsn,
    wal_end pg_lsn,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    duration_seconds double precision,
    attempts integer NOT NULL DEFAULT 1,
    status text NOT NULL DEFAULT 'running',
    error text,
    PRIMARY KEY (project_id, batch_no)
);

CREATE OR REPLACE VIEW public.catmaid_skeleton_treenode_edge_rebuild_status AS
WITH done AS (
    SELECT
        project_id,
        count(*) AS batches_done,
        coalesce(sum(rows_selected), 0)::bigint AS rows_done,
        min(started_at) AS first_started_at,
        max(finished_at) AS last_finished_at,
        sum(duration_seconds) AS work_seconds
    FROM public.catmaid_skeleton_treenode_edge_rebuild_progress
    WHERE status = 'done'
    GROUP BY project_id
),
running AS (
    SELECT DISTINCT ON (project_id)
        project_id,
        batch_no AS running_batch_no,
        id_min AS running_id_min,
        id_max AS running_id_max,
        rows_selected AS running_rows_selected,
        started_at AS running_started_at,
        attempts AS running_attempts
    FROM public.catmaid_skeleton_treenode_edge_rebuild_progress
    WHERE status = 'running'
    ORDER BY project_id, batch_no
)
SELECT
    s.project_id,
    s.status,
    s.batch_size,
    s.target_rows,
    coalesce(d.batches_done, 0) AS batches_done,
    coalesce(d.rows_done, 0) AS rows_done,
    CASE
        WHEN s.target_rows IS NULL OR s.target_rows = 0 THEN NULL
        ELSE round((coalesce(d.rows_done, 0)::numeric / s.target_rows::numeric) * 100, 4)
    END AS percent_done,
    CASE
        WHEN d.work_seconds IS NULL OR d.work_seconds = 0 THEN NULL
        ELSE round((d.rows_done::numeric / d.work_seconds::numeric), 2)
    END AS rows_per_second,
    CASE
        WHEN s.target_rows IS NULL OR d.work_seconds IS NULL OR d.work_seconds = 0 THEN NULL
        ELSE make_interval(secs => greatest(0, ((s.target_rows - d.rows_done)::numeric / (d.rows_done::numeric / d.work_seconds::numeric)))::double precision)
    END AS eta,
    r.running_batch_no,
    r.running_id_min,
    r.running_id_max,
    r.running_rows_selected,
    r.running_started_at,
    r.running_attempts,
    s.started_at,
    d.last_finished_at,
    s.updated_at,
    s.completed_at,
    s.last_error
FROM public.catmaid_skeleton_treenode_edge_rebuild_state s
LEFT JOIN done d ON d.project_id = s.project_id
LEFT JOIN running r ON r.project_id = s.project_id;

CREATE OR REPLACE PROCEDURE public.catmaid_skeleton_rebuild_treenode_edges_batched(
    p_project_id integer,
    p_batch_size integer DEFAULT 250000,
    p_target_rows bigint DEFAULT NULL
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_lock_key bigint;
    v_have_lock boolean;
    v_batch_no bigint;
    v_last_done_id bigint;
    v_id_min bigint;
    v_id_max bigint;
    v_rows_selected bigint;
    v_rows_deleted bigint;
    v_rows_inserted_nonroot bigint;
    v_rows_inserted_root bigint;
    v_rows_inserted_total bigint;
    v_wal_start pg_lsn;
    v_wal_end pg_lsn;
    v_batch_started_at timestamptz;
    v_duration_seconds double precision;
    v_rows_done bigint;
    v_work_seconds double precision;
    v_rows_per_second double precision;
    v_eta_seconds double precision;
BEGIN
    v_lock_key := hashtextextended('catmaid_skeleton_treenode_edge_rebuild:' || p_project_id::text, 0);
    SELECT pg_try_advisory_lock(v_lock_key) INTO v_have_lock;
    IF NOT v_have_lock THEN
        RAISE EXCEPTION 'Another treenode edge rebuild is already running for project %', p_project_id;
    END IF;

    INSERT INTO public.catmaid_skeleton_treenode_edge_rebuild_state (
        project_id,
        batch_size,
        target_rows,
        status,
        started_at,
        updated_at,
        completed_at,
        last_error
    )
    VALUES (
        p_project_id,
        p_batch_size,
        p_target_rows,
        'running',
        clock_timestamp(),
        clock_timestamp(),
        NULL,
        NULL
    )
    ON CONFLICT (project_id) DO UPDATE SET
        batch_size = EXCLUDED.batch_size,
        target_rows = coalesce(EXCLUDED.target_rows, public.catmaid_skeleton_treenode_edge_rebuild_state.target_rows),
        status = 'running',
        updated_at = clock_timestamp(),
        completed_at = NULL,
        last_error = NULL;

    COMMIT;

    LOOP
        SELECT
            batch_no,
            id_min,
            id_max,
            rows_selected
        INTO
            v_batch_no,
            v_id_min,
            v_id_max,
            v_rows_selected
        FROM public.catmaid_skeleton_treenode_edge_rebuild_progress
        WHERE project_id = p_project_id
          AND status = 'running'
        ORDER BY batch_no
        LIMIT 1;

        IF FOUND THEN
            UPDATE public.catmaid_skeleton_treenode_edge_rebuild_progress
            SET
                started_at = clock_timestamp(),
                finished_at = NULL,
                duration_seconds = NULL,
                rows_deleted = NULL,
                rows_inserted_nonroot = NULL,
                rows_inserted_root = NULL,
                wal_start = NULL,
                wal_end = NULL,
                attempts = attempts + 1,
                error = NULL
            WHERE project_id = p_project_id
              AND batch_no = v_batch_no;

            RAISE NOTICE 'Retrying project %, batch %, id range %..%, rows %',
                p_project_id, v_batch_no, v_id_min, v_id_max, v_rows_selected;
        ELSE
            SELECT
                coalesce(max(id_max), 0),
                coalesce(max(batch_no), 0) + 1
            INTO
                v_last_done_id,
                v_batch_no
            FROM public.catmaid_skeleton_treenode_edge_rebuild_progress
            WHERE project_id = p_project_id
              AND status = 'done';

            CREATE TEMP TABLE edge_batch_ids (
                id bigint PRIMARY KEY
            ) ON COMMIT DROP;

            INSERT INTO edge_batch_ids (id)
            SELECT id
            FROM treenode
            WHERE project_id = p_project_id
              AND id > v_last_done_id
            ORDER BY id
            LIMIT p_batch_size;

            GET DIAGNOSTICS v_rows_selected = ROW_COUNT;

            IF v_rows_selected = 0 THEN
                UPDATE public.catmaid_skeleton_treenode_edge_rebuild_state
                SET
                    status = 'complete',
                    updated_at = clock_timestamp(),
                    completed_at = clock_timestamp(),
                    last_error = NULL
                WHERE project_id = p_project_id;

                COMMIT;

                RAISE NOTICE 'No remaining treenodes for project %. Rebuild complete.', p_project_id;
                EXIT;
            END IF;

            SELECT min(id), max(id)
            INTO v_id_min, v_id_max
            FROM edge_batch_ids;

            INSERT INTO public.catmaid_skeleton_treenode_edge_rebuild_progress (
                project_id,
                batch_no,
                id_min,
                id_max,
                rows_selected,
                started_at,
                attempts,
                status
            )
            VALUES (
                p_project_id,
                v_batch_no,
                v_id_min,
                v_id_max,
                v_rows_selected,
                clock_timestamp(),
                1,
                'running'
            );

            RAISE NOTICE 'Prepared project %, batch %, id range %..%, rows %',
                p_project_id, v_batch_no, v_id_min, v_id_max, v_rows_selected;
        END IF;

        COMMIT;

        v_batch_started_at := clock_timestamp();
        v_wal_start := pg_current_wal_lsn();

        DELETE FROM treenode_edge e
        WHERE e.project_id = p_project_id
          AND e.id BETWEEN v_id_min AND v_id_max;
        GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;

        INSERT INTO treenode_edge (id, parent_id, project_id, edge)
        SELECT
            c.id,
            c.parent_id,
            c.project_id,
            ST_MakeLine(
                ST_MakePoint(c.location_x, c.location_y, c.location_z),
                ST_MakePoint(p.location_x, p.location_y, p.location_z)
            )
        FROM treenode c
        JOIN treenode p ON c.parent_id = p.id
        WHERE c.project_id = p_project_id
          AND c.parent_id IS NOT NULL
          AND c.id BETWEEN v_id_min AND v_id_max;
        GET DIAGNOSTICS v_rows_inserted_nonroot = ROW_COUNT;

        INSERT INTO treenode_edge (id, parent_id, project_id, edge)
        SELECT
            r.id,
            r.parent_id,
            r.project_id,
            ST_MakeLine(
                ST_MakePoint(r.location_x, r.location_y, r.location_z),
                ST_MakePoint(r.location_x, r.location_y, r.location_z)
            )
        FROM treenode r
        WHERE r.project_id = p_project_id
          AND r.parent_id IS NULL
          AND r.id BETWEEN v_id_min AND v_id_max;
        GET DIAGNOSTICS v_rows_inserted_root = ROW_COUNT;

        v_rows_inserted_total := v_rows_inserted_nonroot + v_rows_inserted_root;

        IF v_rows_inserted_total <> v_rows_selected THEN
            RAISE EXCEPTION 'Batch % selected % rows, but inserted % rows (% non-root, % root)',
                v_batch_no,
                v_rows_selected,
                v_rows_inserted_total,
                v_rows_inserted_nonroot,
                v_rows_inserted_root;
        END IF;

        v_wal_end := pg_current_wal_lsn();
        v_duration_seconds := extract(epoch FROM (clock_timestamp() - v_batch_started_at));

        UPDATE public.catmaid_skeleton_treenode_edge_rebuild_progress
        SET
            rows_deleted = v_rows_deleted,
            rows_inserted_nonroot = v_rows_inserted_nonroot,
            rows_inserted_root = v_rows_inserted_root,
            wal_start = v_wal_start,
            wal_end = v_wal_end,
            finished_at = clock_timestamp(),
            duration_seconds = v_duration_seconds,
            status = 'done',
            error = NULL
        WHERE project_id = p_project_id
          AND batch_no = v_batch_no;

        UPDATE public.catmaid_skeleton_treenode_edge_rebuild_state
        SET updated_at = clock_timestamp()
        WHERE project_id = p_project_id;

        SELECT
            coalesce(sum(rows_selected), 0),
            coalesce(sum(duration_seconds), 0)
        INTO
            v_rows_done,
            v_work_seconds
        FROM public.catmaid_skeleton_treenode_edge_rebuild_progress
        WHERE project_id = p_project_id
          AND status = 'done';

        IF v_work_seconds > 0 THEN
            v_rows_per_second := v_rows_done / v_work_seconds;
        ELSE
            v_rows_per_second := NULL;
        END IF;

        IF p_target_rows IS NOT NULL AND v_rows_per_second IS NOT NULL AND v_rows_per_second > 0 THEN
            v_eta_seconds := greatest(0, (p_target_rows - v_rows_done) / v_rows_per_second);
        ELSE
            v_eta_seconds := NULL;
        END IF;

        COMMIT;

        RAISE NOTICE 'Done project %, batch %, rows %, deleted %, duration %.2fs, total done %, rate % rows/s, ETA %',
            p_project_id,
            v_batch_no,
            v_rows_selected,
            v_rows_deleted,
            v_duration_seconds,
            v_rows_done,
            round(coalesce(v_rows_per_second, 0)::numeric, 2),
            CASE
                WHEN v_eta_seconds IS NULL THEN 'unknown'
                ELSE (make_interval(secs => v_eta_seconds)::text)
            END;
    END LOOP;

    PERFORM pg_advisory_unlock(v_lock_key);
END;
$procedure$;
