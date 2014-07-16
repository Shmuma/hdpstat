from django.utils import unittest

from sliding_averager import SlidingWindowAverager, SlidingWindowAveragerState


class TestSlidingWindowAverager (unittest.TestCase):
    def test_update(self):
        avg = SlidingWindowAverager(10)
        self.assertEqual(avg.window_len, 10)

        state = SlidingWindowAveragerState()
        val = avg.update(1.0, state)
        self.assertAlmostEqual(state.state(), (1.0, 1, 1.0, 1))
        self.assertAlmostEqual(val, 1.0)

        # reset state
        state = SlidingWindowAveragerState()
        for i in range(1, 10):
            val = avg.update(i, state)
        self.assertAlmostEqual(val, 5)
        self.assertAlmostEqual(state.state(), (45, 9, 30, 4))
        
