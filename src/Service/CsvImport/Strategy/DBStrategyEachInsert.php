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

    public function insert(string $table, array $rows, array $columns, callable $rowCallback = null): ImportResult
    {
        $failedRows = [];

        foreach($rows as $row) {
            $rowsSQL = [];
            $bindings = [];
            $params = [];

            if($rowCallback)
                $row = $rowCallback($row);

            foreach($columns as $column => $valueKey){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey];
                } else {
                    $param = ':' . md5($column);
                    $params[] = $param;

                    $bindings[$param] = $row[$valueKey]; 
                }
            }

            $rowsSQL[] = '(' . implode(', ', $params) . ')';

            $sql = 
            'INSERT INTO ' . $table . ' (' . implode(', ', array_keys($columns)) . ')
            VALUES ' .implode(', ', $rowsSQL) . ' AS new
            ON DUPLICATE KEY UPDATE
            strProductName = new.strProductName,
            strProductDesc = new.strProductDesc,
            intStock = new.intStock,
            decCost = new.decCost';

            $statement = $this->connection->prepare($sql);

            foreach($bindings as $param => $val){
                $statement->bindValue($param, $val);
            }

            try {
                $statement->execute();   
            } catch (\Exception $e) {
                $failedRows[] = $row;
            }
        }

        return new ImportResult($failedRows);
    }
}