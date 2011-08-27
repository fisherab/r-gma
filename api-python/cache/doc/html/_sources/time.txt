Time Classes
============

.. currentmodule:: rgma

Time intervals are handled in a uniform manner using the :class:`TimeInterval` and :class:`TimeUnit` classes.

TimeInterval
------------

.. class:: TimeInterval(value, units=TimeUnit.SECONDS)

    Encapsulates a time value and the units being used.

    Time intervals may not be negative
    
    :arg value: length of the time interval
    :type value: `int`
    :arg units: the units of the measurement
    :type units: :class:`rgma.TimeUnit`

    .. method:: getValueAs([units=TimeUnit.SECONDS])
    
    Returns the length of the time interval in the specified units.
    
    :arg units: the units of the measurement
    :type units: :class:`rgma.TimeUnit`
    :returns: an int with the length of the time interval in the specified units

TimeUnit
--------

.. class:: TimeUnit
 
    Time units of various lengths - each represented by a class variable.
    
    .. attribute:: TimeUnit.SECONDS
    
    A time unit of seconds
    
    .. attribute:: TimeUnit.MINUTES
    
    A time unit of minutes
    
    .. attribute:: TimeUnit.HOURS
    
    A time unit of hours
    
    .. attribute:: DAYS
    
    A time unit of days
