import logging
import time
from logging.handlers import RotatingFileHandler
import inspect

# Merge portalocker to one single log module file
# portalocker.py - Cross-platform (posix/nt) API for flock-style file locking.
#                  Requires python 1.5.2 or better.
"""Cross-platform (posix/nt) API for flock-style file locking.

Synopsis:

   import portalocker
   file = open("somefile", "r+")
   portalocker.lock(file, portalocker.LOCK_EX)
   file.seek(12)
   file.write("foo")
   file.close()

If you know what you're doing, you may choose to

   portalocker.unlock(file)

before closing the file, but why?

Methods:

   lock( file, flags )
   unlock( file )

Constants:

   LOCK_EX
   LOCK_SH
   LOCK_NB

Exceptions:

    LockException

Notes:

For the 'nt' platform, this module requires the Python Extensions for Windows.
Be aware that this may not work as expected on Windows 95/98/ME.

History:

I learned the win32 technique for locking files from sample code
provided by John Nielsen <nielsenjf@my-deja.com> in the documentation
that accompanies the win32 modules.

Author: Jonathan Feinberg <jdf@pobox.com>,
        Lowell Alleman <lalleman@mfps.com>
Version: $Id: portalocker.py 5474 2008-05-16 20:53:50Z lowell $

"""
"""
Add by Karl
This module needs win32api for python.
You can find it here: http://sourceforge.net/projects/pywin32/files%2Fpywin32/
Download the right version according to your OS(32/64bit) and
Python(2.5/2.6/2.7/../3.x/etc) version.
"""


__all__ = [
    "lock",
    "unlock",
    "LOCK_EX",
    "LOCK_SH",
    "LOCK_NB",
    "LockException",
]

import os


class LockException(Exception):
    # Error codes:
    LOCK_FAILED = 1

if os.name == 'nt':
    import win32con
    import win32file
    import pywintypes
    LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
    LOCK_SH = 0  # the default
    LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
    # is there any reason not to reuse the following structure?
    __overlapped = pywintypes.OVERLAPPED()
elif os.name == 'posix':
    import fcntl
    LOCK_EX = fcntl.LOCK_EX
    LOCK_SH = fcntl.LOCK_SH
    LOCK_NB = fcntl.LOCK_NB
else:
    raise RuntimeError("PortaLocker only defined for nt and posix platforms")

if os.name == 'nt':
    def lock(file, flags):
        hfile = win32file._get_osfhandle(file.fileno())
        try:
            win32file.LockFileEx(hfile, flags, 0, -0x10000, __overlapped)
        except pywintypes.error as exc_value:
            # error: (33, 'LockFileEx', 'The process cannot access the file
            # because another process has locked a portion of the file.')
            if exc_value[0] == 33:
                raise LockException(LockException.LOCK_FAILED, exc_value[2])
            else:
                # Q:  Are there exceptions/codes we should be dealing with
                # here?
                raise

    def unlock(file):
        hfile = win32file._get_osfhandle(file.fileno())
        try:
            win32file.UnlockFileEx(hfile, 0, -0x10000, __overlapped)
        except pywintypes.error as exc_value:
            if exc_value[0] == 158:
                # error: (158, 'UnlockFileEx', 'The segment is already unlocked.')
                # To match the 'posix' implementation, silently ignore this
                # error
                pass
            else:
                # Q:  Are there exceptions/codes we should be dealing with
                # here?
                raise

elif os.name == 'posix':
    def lock(file, flags):
        try:
            fcntl.flock(file.fileno(), flags)
        except IOError as exc_value:
            #  IOError: [Errno 11] Resource temporarily unavailable
            if exc_value[0] == 11:
                raise LockException(LockException.LOCK_FAILED, exc_value[1])
            else:
                raise

    def unlock(file):
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


class Log:

    def __init__(self, name, console=1, logfile=None, show_details=False):
        """
        Initialize the Log

        :type name: string
        :param name: the logger name, this param should be different for different loggers

        :type console: int
        :param console: whether output the log to the console, value should be 0 or 1

        :type logfile: string
        :param logfile: the file to save the logs

        :rtype: class object
        :return: a log object
        """

        self._level = logging.DEBUG
        self._mode = "a"
        self._max_bytes = 10 * 1024 * 1024
        self._rotate_count = 5
        self._log_file = logfile
        self._console = console
        self._lock_file = None
        self._fp = None

        self._logger = logging.getLogger(name)
        self._logger.setLevel(self._level)
    
        self._show_details = show_details
            
        logging.Formatter.converter = time.gmtime
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(threadName)s %(message)s")

        if self._console == 1:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self._logger.addHandler(stream_handler)

        if self._log_file is not None:
            self._lock_file = logfile + ".lock"

            rotate_handler = RotatingFileHandler(
                filename=self._log_file,
                mode=self._mode,
                maxBytes=self._max_bytes,
                backupCount=self._rotate_count)
            rotate_handler.setFormatter(formatter)
            self._logger.addHandler(rotate_handler)

    def set_debug_level(self):
        """
        Sets the threshold for this logger to debug. Logging messages will all be printed
        """

        if self._logger is not None:
            self._logger.setLevel(logging.DEBUG)

    def set_info_level(self):
        """
        Sets the threshold for this logger to info. Logging messages which are less severe than info will be ignored.
        """

        if self._logger is not None:
            self._logger.setLevel(logging.INFO)

    def set_warning_level(self):
        """
        Sets the threshold for this logger to warning. Logging messages which are less severe than warning will be ignored.
        """

        if self._logger is not None:
            self._logger.setLevel(logging.WARNING)

    def set_error_level(self):
        """
        Sets the threshold for this logger to error. Logging messages which are less severe than error will be ignored.
        """

        if self._logger is not None:
            self._logger.setLevel(logging.ERROR)

    def set_critical_level(self):
        """
        Sets the threshold for this logger to critical. Logging messages which are less severe than critical will be ignored.
        """

        if self._logger is not None:
            self._logger.setLevel(logging.CRITICAL)

    def _lock(self):
        """
        Lock the file
        """
        if self._lock_file is not None:
            self._fp = open(self._lock_file, 'w')
            if self._fp is not None:
                #fcntl.flock(self._fp, fcntl.LOCK_EX)
                #portalocker.lock(self._fp, portalocker.LOCK_EX)
                lock(self._fp, LOCK_EX)

    def _unlock(self):
        """
        Unlock the file
        """
        if self._fp is not None:
            #fcntl.flock(self._fp, fcntl.LOCK_UN)
            # portalocker.unlock(self._fp)
            unlock(self._fp)
            self._fp.close()

    def debug(self, msg, *args, **kwargs):
        """
        Logs a message with level DEBUG on this logger.

        :type msg: string
        :param msg: message format string

        :type args: arguments
        :param args: the arguments which are merged into msg using the string formatting operator

        :type kwargs: not recommended to use
        :param kwargs: not recommended to use
        """

        if self._logger is not None:
            #self._lock()
            self._logger.debug(self.show_detail(msg), *args, **kwargs)
            #self._unlock()

    def info(self, msg, *args, **kwargs):
        """
        Logs a message with level info on this logger.

        :type msg: string
        :param msg: message format string

        :type args: arguments
        :param args: the arguments which are merged into msg using the string formatting operator

        :type kwargs: not recommended to use
        :param kwargs: not recommended to use
        """

        if self._logger is not None:
            #self._lock()
            self._logger.info(self.show_detail(msg), *args, **kwargs)
            #self._unlock()

    def warning(self, msg, *args, **kwargs):
        """
        Logs a message with level warning on this logger.

        :type msg: string
        :param msg: message format string

        :type args: arguments
        :param args: the arguments which are merged into msg using the string formatting operator

        :type kwargs: not recommended to use
        :param kwargs: not recommended to use
        """

        if self._logger is not None:
            #self._lock()
            self._logger.warning(self.show_detail(msg), *args, **kwargs)
            #self._unlock()

    def error(self, msg, *args, **kwargs):
        """
        Logs a message with level error on this logger.

        :type msg: string
        :param msg: message format string

        :type args: arguments
        :param args: the arguments which are merged into msg using the string formatting operator

        :type kwargs: not recommended to use
        :param kwargs: not recommended to use
        """

        if self._logger is not None:
            #self._lock()
            self._logger.error(self.show_detail(msg), *args, **kwargs)
            #self._unlock()

    def critical(self, msg, *args, **kwargs):
        """
        Logs a message with level critical on this logger.

        :type msg: string
        :param msg: message format string

        :type args: arguments
        :param args: the arguments which are merged into msg using the string formatting operator

        :type kwargs: not recommended to use
        :param kwargs: not recommended to use
        """

        if self._logger is not None:
            #self._lock()
            self._logger.critical(self.show_detail(msg), *args, **kwargs)
            #self._unlock()

    def show_detail(self, message):
        """
        return call func name, func line number, and file name.
        :type message: string
        :param message: the message you want to log
        """
        
        if not self._show_details:
            return message
        lastframe = inspect.currentframe().f_back.f_back
        funcName = lastframe.f_code.co_name
        filelineno = lastframe.f_lineno
        fileName = os.path.basename(lastframe.f_code.co_filename)
        return "%s (%s:%i)\t%s" % (
            funcName,
            fileName,
            filelineno,
            message)


# example - How to use the log module
def example():
    # init logfile path
    pwdpath = os.path.dirname(os.path.realpath(__file__))
    logpath = os.path.join(pwdpath, "logs")
    logfile = os.path.join(logpath, "%s.log" % os.path.basename(__file__))

    if not os.path.isdir(logpath):
        os.makedirs(logpath)

    logger = Log("testAzure", logfile=logfile, show_details=True)

    logger.info("logger module info")
    logger.debug("logger module debug")
    logger.warning("logger module warning")
    logger.error("logger module error")
    logger.critical("logger module critical")

# vim: tabstop=4 shiftwidth=4 softtabstop=4
