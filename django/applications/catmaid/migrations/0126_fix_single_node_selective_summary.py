from importlib import import_module

from django.db import migrations


forward = """
    CREATE OR REPLACE FUNCTION refresh_skeleton_summary_table_selectively(skeleton_ids bigint[]) RETURNS void
    LANGUAGE plpgsql AS
    $$
    BEGIN
        WITH filtered_treenode AS (
            SELECT *
            FROM treenode
            JOIN UNNEST(skeleton_ids) query(query_skeleton_id)
                ON query.query_skeleton_id = treenode.skeleton_id
        ), node_data AS (
            SELECT creation.skeleton_id, creation.project_id,
                creation.user_id, creation.creation_time, edit.editor_id,
                edit.edition_time, counter.nodes,
                COALESCE(len.cable_length, 0) AS cable_length
            FROM
            (
              SELECT *, row_number() OVER(PARTITION BY skeleton_id ORDER BY edition_time DESC) AS rn
              FROM filtered_treenode
            ) edit
            JOIN
            (
              SELECT *, row_number() OVER(PARTITION BY skeleton_id ORDER BY creation_time ASC) AS rn
              FROM filtered_treenode
            ) creation
            ON edit.skeleton_id = creation.skeleton_id
            JOIN
            (
              SELECT skeleton_id, COUNT(*) AS nodes FROM filtered_treenode GROUP BY skeleton_id
            ) counter
            ON creation.skeleton_id = counter.skeleton_id
            -- A singleton has a root but no parent-child edge. Retain its
            -- node count and timestamps, with zero cable length.
            LEFT JOIN
            (
              SELECT t1.skeleton_id, SUM(
                ST_3DLength(ST_MakeLine(ARRAY[
                    ST_MakePoint(t1.location_x, t1.location_y, t1.location_z),
                    ST_MakePoint(t2.location_x, t2.location_y, t2.location_z)
                ]::geometry[]))
              ) AS cable_length
              FROM filtered_treenode t1
              JOIN filtered_treenode t2
              ON t1.parent_id = t2.id
              GROUP BY t1.skeleton_id
            ) len
            ON creation.skeleton_id = len.skeleton_id
            WHERE edit.rn = 1 AND creation.rn = 1
        )
        INSERT INTO catmaid_skeleton_summary (skeleton_id,
            project_id, last_summary_update, original_creation_time,
            last_edition_time, last_editor_id, num_nodes, cable_length)
        (
            SELECT d.skeleton_id, d.project_id, now(), d.creation_time,
                d.edition_time, d.editor_id, d.nodes, d.cable_length
            FROM node_data d
        )
        ON CONFLICT (skeleton_id) DO UPDATE
        SET num_nodes = EXCLUDED.num_nodes,
            last_summary_update = EXCLUDED.last_summary_update,
            last_edition_time = EXCLUDED.last_edition_time,
            last_editor_id = EXCLUDED.last_editor_id,
            cable_length = EXCLUDED.cable_length;
    END;
    $$;
"""

# Restore the exact previous definition on rollback. Historical migration SQL
# is immutable; do not edit migration 0076 in already deployed databases.
backward = import_module("catmaid.migrations.0076_fix_skeleton_summary_update_fn").forward


class Migration(migrations.Migration):

    dependencies = [
        ("catmaid", "0125_ensure_models_use_correct_auto_id_field"),
    ]

    operations = [migrations.RunSQL(forward, backward)]
