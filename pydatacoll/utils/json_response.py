try:
    import ujson as json
except ImportError:
    import json
from aiohttp import web


class JSON(web.Response):
    """Serialize response to JSON with aiohttp.web"""
    def __init__(self, data, status=200, reason=None, headers=None):
        body = json.dumps(data, ensure_ascii=False)
        super().__init__(text=body, status=status, reason=reason,
                         headers=headers, content_type='application/json')
