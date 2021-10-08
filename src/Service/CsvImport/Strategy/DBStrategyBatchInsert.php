<?php

namespace App\Service\CsvImport\Strategy;

use Doctrine\ORM\EntityManagerInterface;

use App\Service\CsvImport\Strategy\DBImportStrategyInterface;
use App\Service\CsvImport\ImportResult;
use App\Service\CsvImport\Strategy\DBFunctionConsts;

class DBStrategyBatchInsert implements DBImportStrategyInterface
{
    private $connection;

    private $batchSize = 50;

    public function __construct(EntityManagerInterface $em)
    {
        $this->connection = $em->getConnection();
    }

    private $type = 'batch';

    public function canInsert(string $type)
    {
        return $this->type === $type;
    }

    public function insert(
        string $table, 
        array $rows, 
        array $columns, 
        array $updateColumns = null, 
        $rowCallback = null
        ): ImportResult
    {
        $failedRows = [];

        foreach(array_chunk($rows, $this->batchSize) as $chunk) {
            $this->batch($table, $chunk, $columns, $updateColumns, $rowCallback);

            $failedRowKeys = $this->catchFailedRowKeys();

            if(count($failedRowKeys)) {
                $failedChunkRows = array_intersect_key($chunk, array_fill_keys($failedRowKeys, true));
                $failedRows = array_merge($failedRows, $failedChunkRows);
            }
        }

        return new ImportResult($failedRows);
    }

    private function batch($table, $rows, array $columns, array $updateColumns, callable $rowCallback = null)
    {
        $bindValues = [];
        $bindings = [];

        $updateColumns = array_intersect_key($columns, array_fill_keys($updateColumns, true));
    
        foreach($rows as $key => $row){
            $params = [];

            if($rowCallback)
                $row = $rowCallback($row);

            foreach($columns as $valueKey => $column){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey] ;
                } else {
                    $param = ':' . md5($column) . $key;
                    $params[] = $param;

                    $bindings[$param] = $row[$valueKey] ; 
                }
            }

            $bindValues[] = '(' . implode(', ', $params) . ')';
        }

        $sql = 
        'INSERT IGNORE INTO '. $table .
        ' (' . implode(', ', $columns) . ') VALUES ' .implode(', ', $bindValues) .
        ' AS new ON DUPLICATE KEY UPDATE ' . implode(', ', array_map(function($column) {
            return $column . ' = new.' . $column;
        }, $updateColumns));

        $statement = $this->connection->prepare($sql);

        foreach($bindings as $param => $val){
            $statement->bindValue($param, $val);
        }
        
        $statement->execute();
    }

    private function catchFailedRowKeys(): array
    {
        $keys = [];

        $warnings = $this->connection->query('SHOW WARNINGS')->fetchAll();
        
        foreach($warnings as $warning) {
            if(isset($warning['Message'])) {
                $message = $warning['Message'];
                $messageParts = explode('\' at row ', $message);

                $key = (int)end($messageParts) - 1;

                $keys[] = $key;
            }
        }

        return array_unique($keys);
    }
}