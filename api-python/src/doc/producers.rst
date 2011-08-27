Producers
=========

.. currentmodule:: rgma

Producers come in three types: :class:`PrimaryProducer`,  :class:`SecondaryProducer` and  :class:`OnDemandProducer`

PrimaryProducer
---------------

.. autoclass:: PrimaryProducer
    :members:
    :undoc-members:
    :show-inheritance:

SecondaryProducer
-----------------

.. autoclass:: SecondaryProducer
    :members:
    :undoc-members:
    :show-inheritance:
    
OnDemandProducer
----------------

.. autoclass:: OnDemandProducer
    :members:
    :undoc-members:
    :show-inheritance:

Storage
-------

.. class:: Storage

    Storage location for tuples.

    .. staticmethod:: getDatabaseStorage()
    
    Gets a database storage object. If no logical name is given the storage cannot be reused.
        
    :return: a database :class:`rgma.Storage` object
    
    .. staticmethod:: getMemoryStorage()
    
    Gets a temporary memory storage object. This storage cannot be reused.
        
    :return: a temporary memory :class:`rgma.Storage` object

SupportedQueries
----------------

.. class:: SupportedQueries
 
    Types of query supported by a primary or secondary producer - each represented by a class variable.
    
    .. attribute:: SupportedQueries.C
    
    Continuous queries only.
    
    .. attribute:: SupportedQueries.CH
    
    Continuous and history queries. 
    
    .. attribute:: SupportedQueries.CL
    
    Continuous and latest queries. 
    
    .. attribute:: SupportedQueries.CHL
    
    Continuous, History and Latest queries.

Producer
--------

.. autoclass:: Producer
    :members:
    :undoc-members:
    :show-inheritance:



