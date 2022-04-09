from utils.logging import Logger

class Task:
    def run(self) -> None:
        print('It is running.')

        self.logger = Logger()
