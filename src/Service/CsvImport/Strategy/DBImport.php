<?php

namespace App\Service\CsvImport\Strategy;

use App\Service\CsvImport\ImportResult;

class DBImport
{
    private $strategies;

    public function addStrategy(DBImportStrategyInterface $strategy): void
    {
        $this->strategies[] = $strategy;
    }

    public function insert(string $type, string $table, array $rows, array $columns, callable $rowCallback = null): ImportResult
    {
        foreach ($this->strategies as $strategy) {
            if ($strategy->canInsert($type)) {
                return $strategy->insert($table, $rows, $columns, $rowCallback);
            }
        }
    }
}