class SlidingWindowAverager (object):
    """
    Class performing sliding window average calculation of time series by
    storing data efficiently.

    The main idea is to store sum of full window and partialy-built sum up to
    half of window. When second sum grows up enough, we replace full window sum
    with second one and start to build second from the beginning.

    In fact, this scheme doesn't gives us precise sliding window average, but
    reduces amount of stored data significantly.
    """
    def __init__ (self, window_len):
        self.window_len = window_len

       
    def update(self, value, state):
        """
        Perform update of state, return average value
        """
        if state.half_count >= self.window_len >> 1:
            state.switch()

        state.add(value)
        return state.value()


class SlidingWindowAveragerState (object):
    def __init__ (self, full_sum=0, full_count=0, half_sum=0, half_count=0):
        self.full_sum = full_sum
        self.full_count = full_count
        self.half_sum = half_sum
        self.half_count = half_count


    def add (self, value):
        self.full_sum += value
        self.full_count += 1
        self.half_sum += value
        self.half_count += 1


    def switch (self):
        self.full_sum = self.half_sum
        self.full_count = self.half_count
        self.half_sum = 0
        self.half_count = 0


    def value (self):
        if self.full_count > 0:
            return float(self.full_sum) / self.full_count
        else:
            return None


    def state (self):
        return (self.full_sum, self.full_count, self.half_sum, self.half_count)
