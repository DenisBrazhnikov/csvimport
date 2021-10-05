<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Style\SymfonyStyle;

use Symfony\Component\HttpKernel\KernelInterface;

use Doctrine\ORM\EntityManagerInterface;

use Symfony\Component\Validator\Constraints as Assert;
use Symfony\Component\Validator\Validation;
use Symfony\Component\Validator\Validator\RecursiveValidator;

use Symfony\Component\Serializer\Encoder\CsvEncoder;
use Symfony\Component\Serializer\Serializer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;

use Symfony\Component\Stopwatch\Stopwatch;

#[AsCommand(
    name: 'app:import',
    description: 'Import stock.csv file. Use --execute flag to perform database import',
)]
class CsvImport extends Command
{
    private $io;
    private $em;
    private RecursiveValidator $validator;

    public function __construct(KernelInterface $kernel, EntityManagerInterface $em)
    {
        parent::__construct();

        $this->validator = Validation::createValidator();
        $this->em = $em;
    }

    protected function configure(): void
    {
        $this->addArgument('file', InputArgument::REQUIRED, 'CSV file path');
        $this->addOption('execute', null, InputOption::VALUE_NONE, 'Perform SQL insert validated data');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start('start');

        $io = new SymfonyStyle($input, $output);

        $io->title('Work has been started...');

        $filePath = $input->getArgument('file');

        $io->text('Looking for data file ' . $filePath . ' ...');

        if(!file_exists($filePath)) {
            $io->caution('Can not find CSV file!');

            return Command::FAILURE;
        }

        $io->success('Data file has been found');

        $io->text('Reading CSV data...');
        
        $records = $this->readCsvFile($filePath);

        $io->text(count($records) . ' rows detected. Validating CSV data...');

        $rowsCount = 0;
        $validRowsCount = 0;
        $validRows = [];
        $incorrectRowsCount = 0;
        $incorrectRows = [];
        $skippedRowsCount = 0;
        $skippedRows = [];
        $headers = array_keys($this->rowValidationRules());

        foreach ($records as $row) {
            $rowsCount++;
            $skipRow = false;

            if($this->rowIsValid($row))
                if($this->rowShouldBeSkipped($row)) {
                    $skippedRows[] = $row;
                    $skippedRowsCount++;
                } else {
                    $validRows[] = $row;
                    $validRowsCount++;
                }
            else {
                $incorrectRows[] = $row;
                $incorrectRowsCount++;
            }
        }

        $io->text('Rows proccessed: ' . $rowsCount);
        $io->text('Valid rows: ' . $validRowsCount);
        $io->text('Incorrect rows: ' . $incorrectRowsCount);
        $io->text('Skipped rows: ' . $skippedRowsCount);

        if($skippedRowsCount == $rowsCount) {
            $io->caution('All lines are incorrect. Probably the whole CSV file is broken');

            return Command::FAILURE;
        }

        if($incorrectRowsCount) {
            $io->section('Incorrect rows (' . $incorrectRowsCount . ')');
            $io->table($headers, $incorrectRows);
        }

        if($skippedRowsCount) {
            $io->section('Skipped rows (' . $skippedRowsCount . ')');
            $io->table($headers, $skippedRows);
        }

        if ($input->getOption('execute')) {
            try {
                $this->migrate($output);
            } catch (Exception $e) {
                $io->caution('Migration has failed.');

                return Command::FAILURE;
            }
            
            $io->section('Inserting valid data to database...');

            $failedRows = $this->insertRowsToDatabase($validRows);

            $io->success('Data has been inserted/updated to database');

            $failedRowsCount = count($failedRows);

            if($failedRowsCount) {
                $io->section('Failed rows (' . $failedRowsCount . ')');
                $io->table($headers, $failedRows);
            }
        }

        $io->text($stopwatch->stop('start'));

        return Command::SUCCESS;
    }

    /**
     * Migrate Stock and Cost columns
     * @param OutputInterface $output
     * @return void
     */
    private function migrate(OutputInterface $output): void
    {
        $input = new ArrayInput([]);
        $input->setInteractive(false);

        $command = $this->getApplication()->find('doctrine:migrations:migrate');
        $command->run($input, $output);
    }

    /**
     * Read CSV and return an array of data rows
     * @param string $filePath path to CSV file
     * @return array CSV rows
     */
    private function readCsvFile(string $filePath): array
    {
        $serializer = new Serializer([new ObjectNormalizer], [new CsvEncoder]);
        
        return $serializer->decode(file_get_contents($filePath), 'csv');
    }

    /**
     * Returns an array of row validation rules
     * @return array
     */
    private function rowValidationRules(): array
    {
        return [
            'Product Code' => [
                new Assert\NotNull(),
                new Assert\Length(['min' => 1, 'max' => 10]),
            ],
            'Product Name' => [
                new Assert\NotNull(),
                new Assert\Length(['min' => 1, 'max' => 50]),
            ],
            'Product Description' => [
                new Assert\NotNull(),
                new Assert\Length(['min' => 1, 'max' => 255]),
            ],
            'Stock' => [
                new Assert\NotNull(),
                new Assert\Type('numeric'),
                new Assert\Regex([
                    'pattern' => '/^[0-9]\d*$/',
                    'message' => 'The given field is not an integer type'
                ]),
            ], 
            'Cost in GBP' => [
                new Assert\NotNull(),
                new Assert\Type('numeric'),
                new Assert\Positive()
            ], 
            'Discontinued' => new Assert\Choice(['yes', '']),
        ];
    }

    /**
     * Determines does a specific row meet validation rules
     * @param array $row Row columns
     * @return bool
     */
    private function rowIsValid(array $row):bool
    {
        $validationRules = $this->rowValidationRules();

        foreach($validationRules as $key => $rule) {
            if(!isset($row[$key]))
                return false;
            
            if($this->validator->validate($row[$key], $rule)->count())
                return false;
        }

        return true;
    }

    /**
     * Determines does a specific row should be skipped based on a supplier needs
     * @param array $row Row columns
     * @return bool
     */
    private function rowShouldBeSkipped(array $row): bool
    {
        if($row['Cost in GBP'] < 5 && $row['Stock'] < 10)
            return true;

        if($row['Cost in GBP'] >= 1000)
            return true;

        return false;
    }

    /**
     * Inserts array of valid rows to database
     * @param array $rows Rows
     * @return array An array of rows that have faild MySQL insert
     */
    private function insertRowsToDatabase(array $rows):array
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

        return $failedRows;
    }
}
