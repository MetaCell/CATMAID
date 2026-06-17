CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_summary_stats_refresh_state (
    project_id integer PRIMARY KEY,
    status text NOT NULL DEFAULT 'not_started',
    batch_size integer NOT NULL,
    target_rows bigint,
    rows_done bigint NOT NULL DEFAULT 0,
    started_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    last_error text
);

CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_summary_stats_refresh_progress (
    project_id integer NOT NULL,
    batch_no bigint NOT NULL,
    id_min bigint NOT NULL,
    id_max bigint NOT NULL,
    rows_selected bigint NOT NULL,
    skeleton_rows_touched bigint,
    stats_rows_touched bigint,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    duration_seconds double precision,
    status text NOT NULL DEFAULT 'running',
    error text,
    PRIMARY KEY (project_id, batch_no)
);

CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_summary_partial (
    project_id integer NOT NULL,
    skeleton_id bigint NOT NULL,
    original_creation_time timestamptz NOT NULL,
    last_edition_time timestamptz NOT NULL,
    last_edition_node_id bigint NOT NULL,
    last_editor_id integer NOT NULL,
    num_nodes bigint NOT NULL,
    cable_length double precision NOT NULL,
    PRIMARY KEY (project_id, skeleton_id)
);

CREATE TABLE IF NOT EXISTS public.catmaid_skeleton_project_stats_partial (
    project_id integer NOT NULL,
    user_id integer NOT NULL,
    date timestamptz NOT NULL,
    n_treenodes bigint NOT NULL,
    cable_length double precision NOT NULL,
    PRIMARY KEY (project_id, user_id, date)
);

CREATE OR REPLACE VIEW public.catmaid_skeleton_summary_stats_refresh_status AS
WITH progress AS (
    SELECT
        project_id,
        count(*) FILTER (WHERE status = 'done') AS batches_done,
        coalesce(sum(rows_selected) FILTER (WHERE status = 'done'), 0) AS rows_done,
        coalesce(sum(duration_seconds) FILTER (WHERE status = 'done'), 0) AS duration_done,
        max(finished_at) FILTER (WHERE status = 'done') AS last_finished_at
    FROM public.catmaid_skeleton_summary_stats_refresh_progress
    GROUP BY project_id
), running AS (
    SELECT DISTINCT ON (project_id)
        project_id,
        batch_no,
        id_min,
        id_max,
        rows_selected,
        started_at
    FROM public.catmaid_skeleton_summary_stats_refresh_progress
    WHERE status = 'running'
    ORDER BY project_id, batch_no DESC
)
SELECT
    s.project_id,
    s.status,
    s.batch_size,
    s.target_rows,
    coalesce(p.batches_done, 0) AS batches_done,
    coalesce(p.rows_done, s.rows_done) AS rows_done,
    CASE
        WHEN s.target_rows IS NULL OR s.target_rows = 0 THEN NULL
        ELSE round(100.0 * coalesce(p.rows_done, s.rows_done)::numeric / s.target_rows, 4)
    END AS percent_done,
    CASE
        WHEN coalesce(p.duration_done, 0) = 0 THEN NULL
        ELSE round(coalesce(p.rows_done, s.rows_done)::numeric / p.duration_done::numeric, 2)
    END AS rows_per_second,
    CASE
        WHEN s.target_rows IS NULL OR coalesce(p.duration_done, 0) = 0 THEN NULL
        ELSE make_interval(secs => greatest(0,
            ((s.target_rows - coalesce(p.rows_done, s.rows_done))::double precision /
             nullif((coalesce(p.rows_done, s.rows_done)::double precision / p.duration_done), 0))
        ))
    END AS eta,
    r.batch_no AS running_batch_no,
    r.id_min AS running_id_min,
    r.id_max AS running_id_max,
    r.rows_selected AS running_rows_selected,
    r.started_at AS running_started_at,
    s.started_at,
    p.last_finished_at,
    s.updated_at,
    s.completed_at,
    s.last_error
FROM public.catmaid_skeleton_summary_stats_refresh_state s
LEFT JOIN progress p ON p.project_id = s.project_id
LEFT JOIN running r ON r.project_id = s.project_id;

CREATE OR REPLACE PROCEDURE public.catmaid_skeleton_refresh_summary_stats_by_node_batched(
    p_project_id integer,
    p_batch_size integer DEFAULT 250000,
    p_target_rows bigint DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_key bigint;
    v_batch_no bigint;
    v_selected bigint;
    v_skeleton_rows bigint;
    v_stats_rows bigint;
    v_min_id bigint;
    v_max_id bigint;
    v_last_done_id bigint;
    v_target bigint;
    v_batch_started_at timestamptz;
    v_duration double precision;
BEGIN
    IF p_batch_size <= 0 THEN
        RAISE EXCEPTION 'Batch size must be positive, got %', p_batch_size;
    END IF;

    v_lock_key := hashtextextended('catmaid_skeleton_summary_stats_refresh:' || p_project_id::text, 0);
    IF NOT pg_try_advisory_lock(v_lock_key) THEN
        RAISE EXCEPTION 'Another summary/stat refresh is already running for project %', p_project_id;
    END IF;

    SELECT coalesce(p_target_rows, count(*)) INTO v_target
    FROM treenode
    WHERE project_id = p_project_id;

    INSERT INTO public.catmaid_skeleton_summary_stats_refresh_state (
        project_id, status, batch_size, target_rows, rows_done,
        started_at, updated_at, completed_at, last_error
    )
    VALUES (
        p_project_id, 'running', p_batch_size, v_target, 0,
        now(), now(), NULL, NULL
    )
    ON CONFLICT (project_id) DO UPDATE
    SET status = CASE
            WHEN public.catmaid_skeleton_summary_stats_refresh_state.status = 'complete'
             AND public.catmaid_skeleton_summary_stats_refresh_state.rows_done >= v_target
            THEN public.catmaid_skeleton_summary_stats_refresh_state.status
            ELSE 'running'
        END,
        batch_size = EXCLUDED.batch_size,
        target_rows = v_target,
        updated_at = now(),
        completed_at = CASE
            WHEN public.catmaid_skeleton_summary_stats_refresh_state.status = 'complete'
             AND public.catmaid_skeleton_summary_stats_refresh_state.rows_done >= v_target
            THEN public.catmaid_skeleton_summary_stats_refresh_state.completed_at
            ELSE NULL
        END,
        last_error = NULL;

    IF EXISTS (
        SELECT 1
        FROM public.catmaid_skeleton_summary_stats_refresh_state
        WHERE project_id = p_project_id
          AND status = 'complete'
          AND rows_done >= v_target
    ) THEN
        RAISE NOTICE 'Summary/stat refresh already complete for project %', p_project_id;
        PERFORM pg_advisory_unlock(v_lock_key);
        RETURN;
    END IF;

    UPDATE public.catmaid_skeleton_summary_stats_refresh_progress
    SET status = 'abandoned',
        finished_at = coalesce(finished_at, clock_timestamp()),
        duration_seconds = coalesce(duration_seconds, extract(epoch FROM clock_timestamp() - started_at)),
        error = 'abandoned by a later refresh attempt'
    WHERE project_id = p_project_id
      AND status = 'running';

    IF NOT EXISTS (
        SELECT 1
        FROM public.catmaid_skeleton_summary_stats_refresh_progress
        WHERE project_id = p_project_id
          AND status = 'done'
    ) THEN
        DELETE FROM public.catmaid_skeleton_summary_stats_refresh_progress
        WHERE project_id = p_project_id;
        DELETE FROM public.catmaid_skeleton_summary_partial
        WHERE project_id = p_project_id;
        DELETE FROM public.catmaid_skeleton_project_stats_partial
        WHERE project_id = p_project_id;

        UPDATE public.catmaid_skeleton_summary_stats_refresh_state
        SET rows_done = 0,
            updated_at = now()
        WHERE project_id = p_project_id;
        COMMIT;
    ELSE
        COMMIT;
    END IF;

    LOOP
        SELECT coalesce(max(id_max), 0)
        INTO v_last_done_id
        FROM public.catmaid_skeleton_summary_stats_refresh_progress
        WHERE project_id = p_project_id
          AND status = 'done';

        SELECT coalesce(max(batch_no), 0) + 1
        INTO v_batch_no
        FROM public.catmaid_skeleton_summary_stats_refresh_progress
        WHERE project_id = p_project_id;

        DROP TABLE IF EXISTS summary_stats_batch_ids;
        CREATE TEMP TABLE summary_stats_batch_ids (
            id bigint PRIMARY KEY
        ) ON COMMIT PRESERVE ROWS;

        INSERT INTO summary_stats_batch_ids (id)
        SELECT id
        FROM treenode
        WHERE project_id = p_project_id
          AND id > v_last_done_id
        ORDER BY id
        LIMIT p_batch_size;

        GET DIAGNOSTICS v_selected = ROW_COUNT;

        IF v_selected = 0 THEN
            DROP TABLE IF EXISTS summary_stats_batch_ids;

            DELETE FROM catmaid_skeleton_summary
            WHERE project_id = p_project_id;

            INSERT INTO catmaid_skeleton_summary (
                skeleton_id, project_id, last_summary_update,
                original_creation_time, last_edition_time, last_editor_id,
                num_nodes, cable_length, num_imported_nodes
            )
            SELECT
                skeleton_id,
                project_id,
                now(),
                original_creation_time,
                last_edition_time,
                last_editor_id,
                num_nodes::integer,
                cable_length,
                num_nodes
            FROM public.catmaid_skeleton_summary_partial
            WHERE project_id = p_project_id;

            DELETE FROM catmaid_stats_summary
            WHERE project_id = p_project_id;

            INSERT INTO catmaid_stats_summary (
                project_id, user_id, date,
                n_treenodes, n_imported_treenodes,
                cable_length, import_cable_length
            )
            SELECT
                project_id,
                user_id,
                date,
                n_treenodes::integer,
                n_treenodes::integer,
                cable_length,
                cable_length
            FROM public.catmaid_skeleton_project_stats_partial
            WHERE project_id = p_project_id;

            ANALYZE catmaid_skeleton_summary;
            ANALYZE catmaid_stats_summary;

            UPDATE public.catmaid_skeleton_summary_stats_refresh_state
            SET status = 'complete',
                rows_done = (
                    SELECT coalesce(sum(rows_selected), 0)
                    FROM public.catmaid_skeleton_summary_stats_refresh_progress
                    WHERE project_id = p_project_id
                      AND status = 'done'
                ),
                updated_at = now(),
                completed_at = now(),
                last_error = NULL
            WHERE project_id = p_project_id;
            COMMIT;

            RAISE NOTICE 'No remaining treenodes for project %. Summary/stat refresh complete.', p_project_id;
            PERFORM pg_advisory_unlock(v_lock_key);
            RETURN;
        END IF;

        SELECT min(id), max(id)
        INTO v_min_id, v_max_id
        FROM summary_stats_batch_ids;

        v_batch_started_at := clock_timestamp();

        INSERT INTO public.catmaid_skeleton_summary_stats_refresh_progress (
            project_id, batch_no, id_min, id_max,
            rows_selected, started_at, status
        )
        VALUES (
            p_project_id, v_batch_no, v_min_id, v_max_id,
            v_selected, v_batch_started_at, 'running'
        );

        COMMIT;

        DROP TABLE IF EXISTS summary_stats_batch_nodes;
        CREATE TEMP TABLE summary_stats_batch_nodes ON COMMIT DROP AS
        SELECT
            t.id,
            t.skeleton_id,
            t.project_id,
            t.user_id,
            t.editor_id,
            t.creation_time,
            t.edition_time,
            date_trunc('hour', t.creation_time) AS stats_date,
            coalesce(ST_3DLength(e.edge), 0)::double precision AS edge_length
        FROM summary_stats_batch_ids b
        JOIN treenode t
          ON t.id = b.id
        LEFT JOIN treenode_edge e
          ON e.id = t.id
         AND e.project_id = p_project_id
        WHERE t.project_id = p_project_id;

        WITH skeleton_agg AS (
            SELECT
                skeleton_id,
                project_id,
                min(creation_time) AS original_creation_time,
                max(edition_time) AS last_edition_time,
                count(*) AS num_nodes,
                sum(edge_length) AS cable_length
            FROM summary_stats_batch_nodes
            GROUP BY skeleton_id, project_id
        ), last_editor AS (
            SELECT DISTINCT ON (skeleton_id)
                skeleton_id,
                id AS last_edition_node_id,
                editor_id AS last_editor_id
            FROM summary_stats_batch_nodes
            ORDER BY skeleton_id, edition_time DESC, id DESC
        )
        INSERT INTO public.catmaid_skeleton_summary_partial (
            project_id, skeleton_id, original_creation_time,
            last_edition_time, last_edition_node_id, last_editor_id,
            num_nodes, cable_length
        )
        SELECT
            s.project_id,
            s.skeleton_id,
            s.original_creation_time,
            s.last_edition_time,
            le.last_edition_node_id,
            le.last_editor_id,
            s.num_nodes,
            s.cable_length
        FROM skeleton_agg s
        JOIN last_editor le ON le.skeleton_id = s.skeleton_id
        ON CONFLICT (project_id, skeleton_id) DO UPDATE
        SET original_creation_time = least(
                public.catmaid_skeleton_summary_partial.original_creation_time,
                EXCLUDED.original_creation_time
            ),
            last_edition_time = greatest(
                public.catmaid_skeleton_summary_partial.last_edition_time,
                EXCLUDED.last_edition_time
            ),
            last_edition_node_id = CASE
                WHEN EXCLUDED.last_edition_time > public.catmaid_skeleton_summary_partial.last_edition_time
                  OR (
                    EXCLUDED.last_edition_time = public.catmaid_skeleton_summary_partial.last_edition_time
                    AND EXCLUDED.last_edition_node_id > public.catmaid_skeleton_summary_partial.last_edition_node_id
                  )
                THEN EXCLUDED.last_edition_node_id
                ELSE public.catmaid_skeleton_summary_partial.last_edition_node_id
            END,
            last_editor_id = CASE
                WHEN EXCLUDED.last_edition_time > public.catmaid_skeleton_summary_partial.last_edition_time
                  OR (
                    EXCLUDED.last_edition_time = public.catmaid_skeleton_summary_partial.last_edition_time
                    AND EXCLUDED.last_edition_node_id > public.catmaid_skeleton_summary_partial.last_edition_node_id
                  )
                THEN EXCLUDED.last_editor_id
                ELSE public.catmaid_skeleton_summary_partial.last_editor_id
            END,
            num_nodes = public.catmaid_skeleton_summary_partial.num_nodes + EXCLUDED.num_nodes,
            cable_length = public.catmaid_skeleton_summary_partial.cable_length + EXCLUDED.cable_length;

        GET DIAGNOSTICS v_skeleton_rows = ROW_COUNT;

        WITH stats_agg AS (
            SELECT
                project_id,
                user_id,
                stats_date AS date,
                count(*) AS n_treenodes,
                sum(edge_length) AS cable_length
            FROM summary_stats_batch_nodes
            GROUP BY project_id, user_id, stats_date
        )
        INSERT INTO public.catmaid_skeleton_project_stats_partial (
            project_id, user_id, date, n_treenodes, cable_length
        )
        SELECT project_id, user_id, date, n_treenodes, cable_length
        FROM stats_agg
        ON CONFLICT (project_id, user_id, date) DO UPDATE
        SET n_treenodes = public.catmaid_skeleton_project_stats_partial.n_treenodes + EXCLUDED.n_treenodes,
            cable_length = public.catmaid_skeleton_project_stats_partial.cable_length + EXCLUDED.cable_length;

        GET DIAGNOSTICS v_stats_rows = ROW_COUNT;

        v_duration := extract(epoch FROM clock_timestamp() - v_batch_started_at);

        UPDATE public.catmaid_skeleton_summary_stats_refresh_progress
        SET skeleton_rows_touched = v_skeleton_rows,
            stats_rows_touched = v_stats_rows,
            finished_at = clock_timestamp(),
            duration_seconds = v_duration,
            status = 'done',
            error = NULL
        WHERE project_id = p_project_id
          AND batch_no = v_batch_no;

        UPDATE public.catmaid_skeleton_summary_stats_refresh_state
        SET rows_done = (
                SELECT coalesce(sum(rows_selected), 0)
                FROM public.catmaid_skeleton_summary_stats_refresh_progress
                WHERE project_id = p_project_id
                  AND status = 'done'
            ),
            updated_at = now()
        WHERE project_id = p_project_id;

        RAISE NOTICE 'Done project %, summary/stat batch %, rows %, duration %.2fs',
            p_project_id, v_batch_no, v_selected, v_duration;

        DROP TABLE IF EXISTS summary_stats_batch_ids;
        COMMIT;
    END LOOP;
END;
$$;
