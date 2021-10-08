<?php

namespace App\Service\CsvImport\Strategy;

use Doctrine\ORM\EntityManagerInterface;

use App\Service\CsvImport\Strategy\DBImportStrategyInterface;
use App\Service\CsvImport\ImportResult;
use App\Service\CsvImport\Strategy\DBFunctionConsts;

class DBStrategyBatchInsert implements DBImportStrategyInterface
{
    private $connection;

    public function __construct(EntityManagerInterface $em)
    {
        $this->connection = $em->getConnection();
    }

    private $type = 'batch';

    public function canInsert(string $type)
    {
        return $this->type === $type;
    }

    /**
     * Full insert or rows by chunking array
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
        int $batchSize = 50
        ): ImportResult
    {
        $failedRows = [];

        foreach(array_chunk($rows, $batchSize) as $chunk) {
            $this->batch($table, $chunk, $columns, $updateColumns, $rowCallback);

            $failedRowKeys = $this->catchFailedRowKeys();

            if(count($failedRowKeys)) {
                // Getting original rows from chunk by keys given from MySQL warnings
                $failedChunkRows = array_intersect_key($chunk, array_fill_keys($failedRowKeys, true));
                $failedRows = array_merge($failedRows, $failedChunkRows);
            }
        }

        return new ImportResult($failedRows);
    }

    /**
     * Inserting full batch of rows
     * @param string $table Table name
     * @param array $rows Array of rows
     * @param array $columns Array of CSV columns(keys) corresponding table columns(values)
     * @param array $updateColumns Array of CSV columns which will be updated in table ON DUPLICATE
     * @param callable $rowCallback A callable to pre-process row before insertion
     */
    private function batch($table, $rows, array $columns, array $updateColumns, callable $rowCallback = null)
    {
        // Here we'll contain an array of () values for MySQL insert query
        $bindValues = [];
        // here we'll contain an array placeholders for pdo statement
        $bindings = [];

        // intersecting columns from CSV to columns in table
        $updateColumns = array_intersect_key($columns, array_fill_keys($updateColumns, true));
    
        foreach($rows as $key => $row){
            $params = [];

            // pre process row if callable been passed
            if($rowCallback)
                $row = $rowCallback($row);

            foreach($columns as $valueKey => $column){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey] ;
                } else {
                    // building placeholder by column name md5 hash to guarantee unique, statement compatible, character-safe string
                    $param = ':' . md5($column) . $key;
                    $params[] = $param;

                    // saving correspondence between placeholder and CSV row value
                    $bindings[$param] = $row[$valueKey] ; 
                }
            }

            // save SQL compatible INSERT VALUES part to array
            $bindValues[] = '(' . implode(', ', $params) . ')';
        }

        $sql = 
        'INSERT IGNORE INTO '. $table .
        ' (' . implode(', ', $columns) . ') VALUES ' .implode(', ', $bindValues) .
        ' AS new ON DUPLICATE KEY UPDATE ' . implode(', ', array_map(function($column) {
            return $column . ' = new.' . $column;
        }, $updateColumns));

        $statement = $this->connection->prepare($sql);

        foreach($bindings as $param => $val) {
            $statement->bindValue($param, $val);
        }
        
        $statement->execute();
    }

    /**
     * Catch failed rows by SHOW WARNINGS statement
     */
    private function catchFailedRowKeys(): array
    {
        $keys = [];

        $warnings = $this->connection->query('SHOW WARNINGS')->fetchAll();
        
        foreach($warnings as $warning) {
            if(isset($warning['Message'])) {
                $message = $warning['Message'];
                $messageParts = explode('\' at row ', $message);

                // we can not parse failed row key on 'NULL' warnings
                if(count($messageParts) > 1) {
                    $key = (int)end($messageParts) - 1;

                    $keys[] = $key;
                }
            }
        }

        return array_unique($keys);
    }
}