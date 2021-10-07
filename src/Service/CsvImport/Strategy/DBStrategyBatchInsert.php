<?php

namespace App\Service\CsvImport\Strategy;

use Doctrine\ORM\EntityManagerInterface;

use App\Service\CsvImport\Strategy\DBImportStrategyInterface;
use App\Service\CsvImport\ImportResult;

class DBStrategyBatchInsert implements DBImportStrategyInterface
{
    private $em;

    public function __construct(EntityManagerInterface $em)
    {
        $this->em = $em;
    }

    private $type = 'batch';

    public function canInsert(string $type)
    {
        return $this->type === $type;
    }

    public function insert(array $rows): ImportResult
    {
        $connection = $this->em->getConnection();

        $statement = $connection->prepare('INSERT INTO tblProductData (
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
        ');

        $failedRows = [];

        foreach($rows as $row) {
            try {
                $statement->bindValue(':code', $row['Product Code']);
                $statement->bindValue(':name', $row['Product Name']);
                $statement->bindValue(':description', $row['Product Description']);
                $statement->bindValue(':stock', $row['Stock']);
                $statement->bindValue(':cost', $row['Cost in GBP']);

                // Set current datetime if the row is discontinued
                $statement->bindValue(':discontinued_at', $row['Discontinued'] ? date('Y-m-d H:i:s') : NULL);

                $statement->execute();
            } catch (\Exception $e) {
                $failedRows[] = $row;
            }
        }

        return new ImportResult($failedRows);
    }
}