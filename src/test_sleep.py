from newio2 import run, sleep


async def main():
    await sleep(3)
    print('end')


if __name__ == '__main__':
    run(main())
