<?php

namespace App\Service\CsvImport\Strategy;

use Doctrine\ORM\EntityManagerInterface;

use App\Service\CsvImport\Strategy\DBImportStrategyInterface;
use App\Service\CsvImport\ImportResult;

class DBStrategyEachInsert implements DBImportStrategyInterface
{
    private $connection;

    public function __construct(EntityManagerInterface $em)
    {
        $this->connection = $em->getConnection();
    }

    private $type = 'each';

    public function canInsert(string $type)
    {
        return $this->type === $type;
    }

    /**
     * Insert all of the rows each by each
     * @param string $table Table name
     * @param array $rows Array of rows
     * @param array $columns Array of CSV columns(keys) corresponding table columns(values)
     * @param array $updateColumns Array of CSV columns which will be updated in table ON DUPLICATE
     * @param callable $rowCallback A callable to pre-process row before insertion
     * @param int $batchSize Batch size
     */
    public function insert(
        string $table, 
        array $rows, 
        array $columns, 
        array $updateColumns = null, 
        callable $rowCallback = null,
        int $batchSize
        ): ImportResult
    {
        $failedRows = [];

        $updateColumns = array_intersect_key($columns, array_fill_keys($updateColumns, true));

        foreach($rows as $row) {
            // here we'll contain an array of () values for MySQL insert query
            $bindValues = [];
            // here we'll contain an array of placeholders for PDO statement
            $bindings = [];
            // here we'll contain an array of row values to wrap them by brackets and add to $bindValues
            $params = [];

            if($rowCallback) {
                $originalRow = $row;
                $row = $rowCallback($row);
            }

            foreach($columns as $valueKey => $column){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey];
                } else {
                    // build character-safe for MySQL statemenet placeholder
                    $param = ':' . md5($column);
                    $params[] = $param;

                    // saving correspondence between placeholders and CSV row values
                    $bindings[$param] = $row[$valueKey]; 
                }
            }

            // adding bracket-wrapped, comma-separated placeholders
            $bindValues[] = '(' . implode(', ', $params) . ')';

            $sql = 
            'INSERT INTO ' . $table .
            '(' . implode(', ', $columns) . ') VALUES ' .implode(', ', $bindValues) .
            ' AS new ON DUPLICATE KEY UPDATE ' . implode(', ', array_map(function($column) {
                return $column . ' = new.' . $column;
            }, $updateColumns));

            $statement = $this->connection->prepare($sql);

            foreach($bindings as $param => $val){
                $statement->bindValue($param, $val);
            }
            
            try {
                $statement->execute();  
            } catch (\Exception $e) {
                $failedRows[] = isset($originalRow) ? $originalRow : $row;
            }
        }

        return new ImportResult($failedRows);
    }
}