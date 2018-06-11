import logging
import faulthandler
faulthandler.enable()
# logging.basicConfig(level='DEBUG')
import time
import newio
from newio_kernel import run


times = """
0.01 0.24633796594571322
0.01 0.24633796594571322
0.01 0.24633796594571322
0.02 0.24628699198365211
0.02 0.24628699198365211
0.02 0.24628699198365211
0.03 0.2462539579719305
0.04 0.24622852099128067
0.05 0.24619706103112549
0.06 0.24617083498742431
0.07 0.24615007697138935
0.01 0.17636457295157015
0.08 0.2461443729698658
0.02 0.17637239897157997
0.09 0.24612986797001213
0.03 0.17637990496587008
0.1 0.24578584101982415
0.04 0.1763947190484032
0.11 0.2457879629218951
0.05 0.17640769202262163
0.12 0.24579766392707825
0.06 0.1764197429874912
0.13 0.24580582603812218
0.07 0.1764327730052173
0.14 0.24582131998613477
1.05 1.853087699972093
0.74 1.5391238100128248
0.65 1.443931336980313
1.06 1.8531061090761796
0.75 1.5391489280154929
0.66 1.4439255290199071
1.07 1.8531201590085402
0.76 1.5391756589524448
0.67 1.4439490749500692
1.08 1.853129858034663
0.77 1.5391983289737254
0.68 1.443974575959146
1.09 1.8531450639711693
0.78 1.5392243779497221
"""

import curio


async def sleep_sensor(seconds, expect):
    begin = time.monotonic()
    await curio.sleep(seconds)
    cost = time.monotonic() - begin
    if cost - seconds > 0.1:
        print(f'{seconds:.3f}, {cost:.3f}, {expect:.3f}')


async def main():
    # async with newio.open_nursery() as nursery:
    async with curio.TaskGroup() as nursery:
        # for line in times.strip().splitlines():
        #     if line.startswith('#'):
        #         continue
        #     seconds, actual = map(float, line.strip().split(' '))
        #     await nursery.spawn(sleep_sensor(seconds, actual))
        for _ in range(10000):
            for i in range(1, 2):
                seconds = 0.010 * i
                await nursery.spawn(sleep_sensor(seconds, seconds))
        await nursery.join()

curio.run(main())
