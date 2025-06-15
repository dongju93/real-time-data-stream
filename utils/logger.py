import atexit
import logging
import logging.handlers
import multiprocessing
import sys


class MultiprocessLogger:
    """
    멀티프로세스 환경에서 안전한 로그 처리를 위한 클래스
    QueueHandler와 QueueListener를 사용하여 race condition을 방지합니다.
    """

    def __init__(self, name: str = "multiprocess_logger", log_file: str = "app.log"):
        self.name = name
        self.log_file = log_file
        self.queue = multiprocessing.Queue(-1)
        self.listener = None
        self._setup_logging()

    def _setup_logging(self):
        """로깅 시스템 초기화"""
        # 메인 로거 설정
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

        # 기존 핸들러 제거 (중복 방지)
        self.logger.handlers.clear()

        # Queue 핸들러 추가 (모든 프로세스에서 사용)
        queue_handler = logging.handlers.QueueHandler(self.queue)
        self.logger.addHandler(queue_handler)

        # 포매터 설정
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - [PID:%(process)d] - %(message)s"
        )

        # 실제 로그 출력을 담당하는 핸들러들
        stream_handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.FileHandler(self.log_file)

        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # QueueListener 생성 (메인 프로세스에서만 실행)
        self.listener = logging.handlers.QueueListener(
            self.queue, stream_handler, file_handler
        )

        # QueueListener 시작
        self.listener.start()

        # 프로그램 종료 시 리스너 정리
        atexit.register(self.stop_listener)

    def stop_listener(self):
        """QueueListener 종료"""
        if self.listener:
            self.listener.stop()

    def get_logger(self) -> logging.Logger:
        """멀티프로세스 안전 로거 반환"""
        return self.logger


# 전역 멀티프로세스 로거 인스턴스
_multiprocess_logger_instance: MultiprocessLogger | None = None


def logger_instance(name: str = "app", log_file: str = "app.log") -> logging.Logger:
    """
    멀티프로세스 환경에서 안전한 로거를 반환하는 헬퍼 함수

    Args:
        name: 로거 이름
        log_file: 로그 파일 경로

    Returns:
        멀티프로세스 안전 로거
    """
    global _multiprocess_logger_instance

    if _multiprocess_logger_instance is None:
        _multiprocess_logger_instance = MultiprocessLogger(name, log_file)

    return _multiprocess_logger_instance.get_logger()
