Consumers
=========

.. currentmodule:: rgma

Consumers obtain data from producers.

Consumer
--------

.. autoclass:: Consumer
    :members:
    :undoc-members:
    :show-inheritance:
    
QueryType
---------
    
.. class:: QueryType

    Permitted query type for a consumer - each represented by a class variable.
    
    .. attribute:: C
    
    Continuous Query.
    
    .. attribute:: H
    
    History Query.
    
    .. attribute:: L
    
    Latest Query.
    
    .. attribute:: S
    
    Static Query.
   
QueryTypeWithInterval
---------------------
   
.. class:: QueryTypeWithInterval

    Permitted query type for a consumer where an interval for the query should be specified - each represented by a class variable.

    .. attribute:: C
    
    Continuous Query.
    
    .. attribute:: H
    
    History Query.
    
    .. attribute:: L
    
    Latest Query.

TupleSet
--------

.. class:: TupleSet

    Results returned by a pop call on a consumer.
    
    .. automethod:: TupleSet.getData
    .. automethod:: TupleSet.getWarning
    .. automethod:: TupleSet.isEndOfResults()
    
Tuple
-----

.. class:: Tuple

    Represents a tuple (or row of a table).
 
    .. automethod:: Tuple.getBool

    .. automethod:: Tuple.getFloat

    .. automethod:: Tuple.getInt

    .. automethod:: Tuple.getString

    .. automethod:: Tuple.isNull

    .. automethod:: Tuple.getPyTuple
 