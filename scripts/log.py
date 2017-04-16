import functools, logging


class log_with(object):
  '''Logging decorator that allows you to log with a
  specific logger.
  '''
  # Customize these messages
  ENTRY_MESSAGE = 'Entering {}; {}; {}'
  EXIT_MESSAGE = 'Exiting {}; {}'

  def __init__(self, logger=None):
    self.logger = logger

  def __call__(self, func):
    '''Returns a wrapper that wraps func.
    The wrapper will log the entry and exit points of the function
    with logging.INFO level.
    '''
    # set logger if it was not set earlier
    if not self.logger:
      logging.basicConfig()
      self.logger = logging.getLogger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args, **kwds):
      try:
        self.logger.info(
          self.ENTRY_MESSAGE.format(func.__name__, args, kwds)
        )
      except:
        self.logger.warn('Cound not log call {}'.format(func.__name__))
        pass
      f_result = func(*args, **kwds)

      try:
        self.logger.info(
          self.EXIT_MESSAGE.format(func.__name__, f_result)
         )
      except:
        self.logger.warn('Cound not log res {}'.format(func.__name__))
        pass
      # logging level .info(). Set to .debug() if you want to
      return f_result
    return wrapper
