"""Names of the CATMAID annotations that link NeuroGlass tracing tasks."""

import re

UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
TASK_ANNOTATION = re.compile(r'neuroglass:study:(%s):task:%s' % (UUID_PATTERN, UUID_PATTERN),
        re.IGNORECASE)
