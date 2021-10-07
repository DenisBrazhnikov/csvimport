<?php

namespace App\Service\CsvImport;

class ValidationResult {
    private $rowsProcessedCount = 0;
    private $validRows = [];
    private $incorrectRows = [];
    private $skippedRows = [];

    public function __construct(int $rowsProcessedCount, array $validRows, array $incorrectRows, array $skippedRows)
    {
        $this->rowsProcessedCount = $rowsProcessedCount;
        $this->validRows = $validRows;
        $this->incorrectRows = $incorrectRows;
        $this->skippedRows = $skippedRows;
    }

    public function getRowsProcessedCount(): int
    {
        return $this->rowsProcessedCount;
    }

    public function getValidRows(): array
    {
        return $this->validRows;
    }

    public function getValidRowsCount(): int
    {
        return count($this->validRows);
    }

    public function getIncorrectRows(): array
    {
        return $this->incorrectRows;
    }

    public function getIncorrectRowsCount(): int
    {
        return count($this->incorrectRows);
    }

    public function getSkippedRows(): array
    {
        return $this->skippedRows;
    }

    public function getSkippedRowsCount(): int
    {
        return count($this->skippedRows);
    }
}