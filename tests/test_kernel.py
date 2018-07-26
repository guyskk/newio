import pytest
from newio import run, spawn, sleep


def test_shutdown():
    async def main():
        for i in range(4):
            await spawn(subtask(i))
        await sleep(1)

    async def subtask(n):
        if n == 3:
            raise KeyboardInterrupt()
        await sleep(1)

    with pytest.raises(KeyboardInterrupt):
        run(main())
