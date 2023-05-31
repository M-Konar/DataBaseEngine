# Database Application in Java

This is a Java code for a database application that allows users to manage tables and perform various operations on them. The code includes functionalities such as reading configuration files, initializing the engine, and managing tables.

## Getting Started

To get started with this database application, you will need to have Java installed on your machine. You can download the latest version of Java from the official website: https://www.java.com/en/download/

Once you have installed Java, you can clone this repository to your local machine using Git:

```
git clone https://github.com/m-abdelgaber/DataBaseEngine
```

## Usage

To use this database application, you can run the main method in the DBApp class. This will initialize the engine and create a directory for storing data.

You can then create tables using the createTable method in the Table class. This method takes two arguments: a string representing the name of the table and a Hashtable representing the column names and their data types.

You can insert records into tables using the insertIntoTable method in the Table class. This method takes two arguments: a string representing the name of the table and a Hashtable representing the column names and their values.

You can update records in tables using the updateTable method in the Table class. This method takes three arguments: a string representing the name of the table, a Hashtable representing the column names and their new values, and a Hashtable representing conditions for selecting records to update.

You can delete records from tables using the deleteFromTable method in the Table class. This method takes two arguments: a string representing the name of the table and a Hashtable representing conditions for selecting records to delete.

You can select records from tables using various methods in the Table class such as selectFromTableByPage, selectFromTableByIndex, and selectFromTableByCondition. These methods take arguments such as the name of the table, the page number, the index bucket number, and conditions for selecting records.

## Contributing

If you would like to contribute to this database application, you can fork this repository and submit a pull request with your changes. Please make sure to follow the coding conventions used in this code and include tests for your changes.
