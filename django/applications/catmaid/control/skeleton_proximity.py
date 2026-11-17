# -*- coding: utf-8 -*-

"""Generic spatial search for skeletons close to given points."""

import math
import sys
from typing import Any, List

from django.db import connection
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse

from rest_framework.decorators import api_view

from catmaid.control.authentication import requires_user_role
from catmaid.models import UserRole

NEAR_POINTS_MAX = 5000
# Keep the search box and squared 3D distances within double precision.
MAX_SPATIAL_MAGNITUDE = math.sqrt(sys.float_info.max) / 4


def _spatial_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('Expected a number')
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError('Number is too large for a spatial search') from exc
    if not math.isfinite(number) or abs(number) > MAX_SPATIAL_MAGNITUDE:
        raise ValueError('Number is outside the range for spatial searches')
    return number


def _parse_request(data: Any):
    """Return (points, radius) or raise ValueError with a message."""
    if not isinstance(data, dict):
        raise ValueError('Expected a proximity request object')
    points = data.get('points')
    if not isinstance(points, list):
        raise ValueError('"points" must be a list of [x, y, z] positions')
    if len(points) > NEAR_POINTS_MAX:
        raise ValueError('At most %d points can be searched at once' %
                NEAR_POINTS_MAX)
    parsed = []
    for point in points:
        if not isinstance(point, (list, tuple)) or len(point) != 3:
            raise ValueError('Every point must be a list of three numbers')
        try:
            xyz = [_spatial_number(c) for c in point]
        except ValueError as exc:
            raise ValueError('Every point must have three finite coordinates in range') from exc
        parsed.append(xyz)
    try:
        radius = _spatial_number(data.get('radius'))
    except ValueError as exc:
        raise ValueError('"radius" must be a positive number in range') from exc
    if radius <= 0:
        raise ValueError('"radius" must be a positive number')
    return parsed, radius


@api_view(['POST'])
@requires_user_role(UserRole.Browse)
def skeletons_near_points(request: HttpRequest, project_id=None) -> JsonResponse:
    """Find all skeletons with a node within a radius of each given point.

    For every input point, one entry is returned per skeleton that has at least
    one node within the radius. Each entry describes the skeleton's node that is
    nearest to the point and the skeleton's current root node. Entries are
    sorted by distance and then skeleton ID. No limit is applied to the number
    of skeletons returned per point.
    ---
    parameters:
    - name: project_id
      description: Project to work in
      required: true
      type: integer
      paramType: path
    - name: points
      description: |
        A list of [x, y, z] locations in project space (nm). At most 5000.
      required: true
      type: array
      paramType: form
    - name: radius
      description: Maximum 3D distance (nm) between a point and a node.
      required: true
      type: number
      paramType: form
    type:
      results:
        type: array
        description: |
          One list per input point, in input order. Each element has the fields
          skeleton_id, treenode_id, distance, x, y, z and root_id.
        required: true
    """
    try:
        points, radius = _parse_request(request.data)
    except ValueError as e:
        return HttpResponseBadRequest(str(e))

    results: List[List[dict]] = [[] for _ in points]
    if not points:
        return JsonResponse({'results': results})

    values_sql = ','.join(['(%s, %s, %s, %s)'] * len(points))
    params: List[Any] = []
    for idx, (x, y, z) in enumerate(points):
        params.extend([idx, x, y, z])
    params.extend([radius] * 7 + [int(project_id)] * 2)

    cursor = connection.cursor()
    cursor.execute('''
        WITH query_point(idx, x, y, z) AS (
            VALUES {values}
        ), candidate AS (
            SELECT DISTINCT ON (qp.idx, t.skeleton_id)
                qp.idx, t.skeleton_id, t.id AS treenode_id,
                ST_3DDistance(ST_StartPoint(te.edge), ST_MakePoint(qp.x, qp.y, qp.z)) AS distance,
                t.location_x, t.location_y, t.location_z
            FROM query_point qp
            JOIN treenode_edge te
                ON te.edge &&& ST_MakeLine(ARRAY[
                    ST_MakePoint(qp.x - %s, qp.y - %s, qp.z - %s),
                    ST_MakePoint(qp.x + %s, qp.y + %s, qp.z + %s)]::geometry[])
               AND ST_3DDWithin(ST_StartPoint(te.edge), ST_MakePoint(qp.x, qp.y, qp.z), %s)
            JOIN treenode t
                ON t.id = te.id
            WHERE te.project_id = %s
            ORDER BY qp.idx, t.skeleton_id, distance, t.id
        )
        SELECT c.idx, c.skeleton_id, c.treenode_id, c.distance,
            c.location_x, c.location_y, c.location_z, r.id
        FROM candidate c
        LEFT JOIN (
            SELECT skeleton_id, MIN(id) AS id
            FROM treenode
            WHERE project_id = %s AND parent_id IS NULL
              AND skeleton_id IN (SELECT skeleton_id FROM candidate)
            GROUP BY skeleton_id
        ) r
            ON r.skeleton_id = c.skeleton_id
        ORDER BY c.idx, c.distance, c.skeleton_id
    '''.format(values=values_sql),
        params)

    for idx, skeleton_id, treenode_id, distance, x, y, z, root_id in cursor.fetchall():
        results[idx].append({
            'skeleton_id': skeleton_id,
            'treenode_id': treenode_id,
            'distance': distance,
            'x': x,
            'y': y,
            'z': z,
            'root_id': root_id,
        })

    return JsonResponse({'results': results})
