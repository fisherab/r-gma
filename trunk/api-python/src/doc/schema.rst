Schema Operations
=================

.. currentmodule:: rgma

The schema holds table definitions

Schema
------

.. autoclass:: Schema
    :members:

    
TableDefinition
--------------- 

.. class:: TableDefinition

    Definitions of a table, including table name and column details.
 
    .. automethod:: TableDefinition.getColumns
    .. automethod:: TableDefinition.getTableName
    .. automethod:: TableDefinition.getViewFor
    .. automethod:: TableDefinition.isView
    
ColumnDefinition
----------------

.. class:: ColumnDefinition

    Definition of a column which may be used inside a :class:`rgma.TableDefinition`.

    .. automethod:: ColumnDefinition.getName
    .. automethod:: ColumnDefinition.getSize
    .. automethod:: ColumnDefinition.getType
    .. automethod:: ColumnDefinition.isNotNull
    .. automethod:: ColumnDefinition.isPrimaryKey
    
Index
-----

.. class:: Index

    Information about an index on a table.
    
    .. automethod:: Index.getIndexName
    .. automethod:: Index.getColumnNames

     
RGMAType
--------

.. class:: RGMAType
 
    Constants for SQL column types.
    
    .. attribute:: RGMAType.INTEGER
    .. attribute:: RGMAType.REAL
    .. attribute:: RGMAType.DOUBLE
    .. attribute:: RGMAType.CHAR
    .. attribute:: RGMAType.VARCHAR
    .. attribute:: RGMAType.TIMESTAMP
    .. attribute:: RGMAType.DATE
    .. attribute:: RGMAType.TIME
    
    
 