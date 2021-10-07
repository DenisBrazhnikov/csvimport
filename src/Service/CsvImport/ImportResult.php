<?php

namespace App\Service\CsvImport;

class ImportResult
{
    private $failedRows;

    public function __construct(array $failedRows = [])
    {
        $this->failedRows = $failedRows;
    }

    public function getFailedRows(): array
    {
        return $this->failedRows;
    }

    public function getFailedRowsCount(): int
    {
        return count($this->failedRows);
    }
}