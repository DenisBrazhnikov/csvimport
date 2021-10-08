<?php

namespace App\Service\CsvImport\Strategy;

use Doctrine\ORM\EntityManagerInterface;

use App\Service\CsvImport\Strategy\DBImportStrategyInterface;
use App\Service\CsvImport\ImportResult;

class DBStrategyEachInsert implements DBImportStrategyInterface
{
    private $connection;

    private $statement = '(
        strProductCode,
        strProductName,
        strProductDesc,
        intStock,
        decCost,
        dtmAdded,
        dtmDiscontinued
    ) VALUES (
        :code,
        :name,
        :description,
        :stock,
        :cost,
        NOW(),
        :discontinued_at
    ) ON DUPLICATE KEY UPDATE
        strProductName = :name,
        strProductDesc = :description,
        intStock = :stock,
        decCost = :cost,
        dtmDiscontinued = :discontinued_at
    ';

    public function __construct(EntityManagerInterface $em)
    {
        $this->connection = $em->getConnection();
    }

    private $type = 'each';

    public function canInsert(string $type)
    {
        return $this->type === $type;
    }

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
            $bindValues = [];
            $bindings = [];
            $params = [];

            if($rowCallback) {
                $originalRow = $row;
                $row = $rowCallback($row);
            }

            foreach($columns as $valueKey => $column){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey];
                } else {
                    $param = ':' . md5($column);
                    $params[] = $param;

                    $bindings[$param] = $row[$valueKey]; 
                }
            }

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