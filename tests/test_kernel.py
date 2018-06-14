import pytest
import logging
import newio as nio
from newio_kernel import run

from .helper import run_it


def test_shutdown():
    async def main():
        for i in range(4):
            await nio.spawn(subtask(i))
        await nio.sleep(1)

    async def subtask(n):
        if n == 3:
            raise KeyboardInterrupt()
        await nio.sleep(1)

    with pytest.raises(KeyboardInterrupt):
        run(main())


@run_it
async def test_cancel_dead_task(caplog):
    async def dead_task():
        while True:
            try:
                await nio.sleep(1)
            except BaseException:
                pass
    task = await nio.spawn(dead_task())
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        await task.cancel()
        await task.join()
    for record in caplog.records:
        assert 'failed to cancel task' in record.message
    assert not task.is_alive
    assert task.error is not None
