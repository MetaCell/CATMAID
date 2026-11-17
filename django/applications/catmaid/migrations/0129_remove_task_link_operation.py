from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('catmaid', '0128_task_link_operation'),
    ]

    operations = [
        migrations.DeleteModel(name='TaskLinkOperation'),
    ]
