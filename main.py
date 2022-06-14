import src.start_env as s
import src.part_1 as p1
import os
import time


def main():
    # start_env: Loads postgres databases with the transactions.
    s.start_env()
    p1.part_1()


if __name__ == "__main__":
    main()
