<?php

namespace App\Service\CsvImport;

use Symfony\Component\Validator\Validation;
use Symfony\Component\Validator\Validator\RecursiveValidator;

use App\Service\CsvImport\ValidationResult;

class RowsValidator
{
    private RecursiveValidator $validator;

    public function __construct()
    {   
        $this->validator = Validation::createValidator();
    }

    /**
     * Validates rows by an array of validation rules
     * @param array $rules Row validation rules
     * @param callable $rowShouldBeSkipped A callable to filter rows by a supplier needs
     * @return ValidationResult
     */
    public function validate(array $rows, array $rules, callable $rowShouldBeSkipped): ValidationResult
    {
        $validRows = [];
        $incorrectRows = [];
        $skippedRows = [];

        $rowsProcessedCount = 0;

        foreach ($rows as $row) {
            $rowsProcessedCount++;
            
            if($this->rowIsValid($row, $rules)){
                if($rowShouldBeSkipped($row))
                    $skippedRows[] = $row;
                else
                    $validRows[] = $row;
            }
            else
                $incorrectRows[] = $row;
        }

        return new ValidationResult($rowsProcessedCount, $validRows, $incorrectRows, $skippedRows);
    }

    /**
     * Determines does a specific row meet validation rules
     * @param array $row Row columns
     * @param array $rules Row validation rules
     * @return bool
     */
    private function rowIsValid(array $row, array $rules): bool
    {
        foreach($rules as $key => $rule) {
            if(!isset($row[$key]))
                return false;
            
            if($this->validator->validate($row[$key], $rule)->count())
                return false;
        }

        return true;
    }
}