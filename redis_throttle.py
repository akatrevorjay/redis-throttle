import time


class RedisThrottle(object):
    expiration_window = 10

    def __init__(self, r, key_prefix, limit, over_time):
        self.r = r
        self.key_prefix = key_prefix
        self.limit = limit
        self.over_time = over_time
        # This being zero forces an update on self.key
        self.reset_at = 0

    @property
    def key(self):
        if time.time() > self.reset_at:
            self.update()
        return self._key

    def update(self, ts=None):
        if not ts:
            ts = time.time()
        self.reset_at = int((ts // self.over_time) * self.over_time + self.over_time)
        self._key = '%s/slot=%s' % (self.key_prefix, self.reset_at)

    def incr(self):
        with self.r.pipeline() as p:
            p.multi()
            p.incr(self.key)
            p.expireat(self.key, self.reset_at + self.expiration_window)
            # self._current = min(p.execute()[0], self.limit)
            self._current = p.execute()[0]
            return self._current

    _current = None

    @property
    def current(self):
        if self._current is None:
            self.get_current()
        return self._current

    def get_current(self):
        self._current = int(self.r.get(self.key) or 0)
        # _LOG.debug('%s %s %s %s %s %s', self, self.key, self.r.get(self.key), self.limit, self.over_time, self._current)
        return self._current

    remaining = property(lambda self: self.limit - self.current)
    over_limit = property(lambda self: self.current >= self.limit)

    def __str__(self):
        return '%s/%s[%s]' % (self.limit, self.over_time, self.current)
