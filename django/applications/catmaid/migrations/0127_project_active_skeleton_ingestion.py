"""Add the lock owner to the live project and its CATMAID history table/view."""
from django.db import migrations, models

PREPARE = """
SELECT disable_history_tracking_for_table('project'::regclass, get_history_table_name('project'::regclass));
SELECT drop_history_view_for_table('project'::regclass);
"""
RESTORE = """
SELECT create_history_view_for_table('project'::regclass);
SELECT enable_history_tracking_for_table('project'::regclass, get_history_table_name('project'::regclass), FALSE);
"""
FORWARD = PREPARE + """
ALTER TABLE project ADD COLUMN active_skeleton_ingestion uuid NULL;
ALTER TABLE project__history ADD COLUMN active_skeleton_ingestion uuid NULL;
""" + RESTORE
BACKWARD = """
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM project WHERE active_skeleton_ingestion IS NOT NULL) THEN
        RAISE EXCEPTION 'Resolve project ingestion locks before reversing this migration';
    END IF;
END $$;
""" + PREPARE + """
ALTER TABLE project DROP COLUMN active_skeleton_ingestion;
ALTER TABLE project__history DROP COLUMN active_skeleton_ingestion;
""" + RESTORE


class Migration(migrations.Migration):
    dependencies = [("catmaid", "0126_fix_single_node_selective_summary")]
    operations = [migrations.RunSQL(FORWARD, BACKWARD, state_operations=[
        migrations.AddField(
            model_name="project", name="active_skeleton_ingestion",
            field=models.UUIDField(null=True, blank=True, editable=False),
        ),
    ])]
