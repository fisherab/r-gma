Exceptions
==========

.. currentmodule:: rgma

For resilience of client code it is important to deal with exceptions correclty.
The general style of programming should be to print a message and give up when 
receiving an RGMAPermanentException and to try again after an 
RGMATemporaryException.

RGMAPermanentException
----------------------

.. autoexception:: RGMAPermanentException
    :members:
    :show-inheritance:

RGMATemporaryException
----------------------

.. autoexception:: RGMATemporaryException
    :members:
    :show-inheritance:

RGMAException
-------------

.. autoexception:: RGMAException
    :members:
    :show-inheritance:



