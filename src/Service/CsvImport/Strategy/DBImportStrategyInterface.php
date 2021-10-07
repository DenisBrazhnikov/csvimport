<?php

namespace App\Service\CsvImport\Strategy;

interface DBImportStrategyInterface
{
    public const SERVICE_TAG = 'db_insert_strategy';

    public function canInsert(string $type);
    public function insert(array $rows);
}