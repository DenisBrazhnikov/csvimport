<?php

namespace App\Service\CsvImport\Strategy;

interface DBImportStrategyInterface
{
    public const SERVICE_TAG = 'db_insert_strategy';

    public function canInsert(string $type);
    public function insert(string $table, array $rows, array $columns, array $updateColumns, callable $rowCallable, int $batchSize);
}