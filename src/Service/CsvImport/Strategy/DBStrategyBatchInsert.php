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

    public function insert(string $table, array $rows, array $columns, $rowCallback = null): ImportResult
    {
        foreach(array_chunk($rows, $this->batchSize) as $chunk) {
            $this->batch($table, $chunk, $columns, $rowCallback);

            $stm = $this->connection->query('SHOW WARNINGS');
            //var_dump($stm->fetchAll());
        }

        return new ImportResult([]);
    }

    private function batch($table, $rows, array $columns, callable $rowCallback = null)
    {
        $rowsSQL = [];
        $bindings = [];
    
        foreach($rows as $key => $row){
            $params = [];

            if($rowCallback)
                $row = $rowCallback($row);

            foreach($columns as $column => $valueKey){
                if($row[$valueKey] instanceof DBRawFunction) {
                    $params[] = $row[$valueKey] ;
                } else {
                    $param = ':' . md5($column) . $key;
                    $params[] = $param;

                    $bindings[$param] = $row[$valueKey] ; 
                }
            }

            $rowsSQL[] = '(' . implode(', ', $params) . ')';
        }

        $sql = 
        'INSERT IGNORE INTO '. $table .
        ' (' . implode(', ', array_keys($columns)) . ') VALUES ' .
        implode(', ', $rowsSQL) .
        ' AS new ON DUPLICATE KEY UPDATE
        strProductName = new.strProductName,
        strProductDesc = new.strProductDesc,
        intStock = new.intStock,
        decCost = new.decCost';

        $statement = $this->connection->prepare($sql);

        foreach($bindings as $param => $val){
            $statement->bindValue($param, $val);
        }
        
        return $statement->execute();
    }
}