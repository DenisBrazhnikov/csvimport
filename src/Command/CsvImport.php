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

use Symfony\Component\Validator\Constraints as Assert;

use App\Service\CsvImport\CsvParser;
use App\Service\CsvImport\RowsValidator;
use App\Service\CsvImport\Strategy\DBImport;
use App\Service\CsvImport\Strategy\DBRawFunction;

use Symfony\Component\Stopwatch\Stopwatch;

#[AsCommand(
    name: 'app:import',
    description: 'Import stock.csv file. Use --execute flag to perform database import',
)]
class CsvImport extends Command
{
    private $csvParser;
    private $rowsValidator;
    private $dbImport;

    public function __construct(CsvParser $csvParser, RowsValidator $rowsValidator, DBImport $dbImport)
    {
        parent::__construct();

        $this->csvParser = $csvParser;
        $this->rowsValidator = $rowsValidator;
        $this->dbImport = $dbImport;
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
        
        $rows = $this->csvParser->serializeFile($filePath);

        $io->text(count($rows) . ' rows detected. Validating CSV data...');

        $headers = array_keys($this->rowValidationRules());

        $validatorResult = $this->rowsValidator->validate($rows, $this->rowValidationRules(), function ($row) {
            if($row['Cost in GBP'] < 5 && $row['Stock'] < 10)
                return true;
    
            if($row['Cost in GBP'] >= 1000)
                return true;
    
            return false;
        });

        $io->text('Rows processed: ' . $validatorResult->getRowsProcessedCount());
        $io->text('Valid rows: ' . $validatorResult->getValidRowsCount());
        $io->text('Incorrect rows: ' . $validatorResult->getIncorrectRowsCount());
        $io->text('Skipped rows: ' . $validatorResult->getSkippedRowsCount());

        if(!$validatorResult->getValidRowsCount()) {
            $io->caution('CSV file is broken');

            return Command::FAILURE;
        }

        if($validatorResult->getIncorrectRowsCount()) {
            $io->section('Incorrect rows (' . $validatorResult->getIncorrectRowsCount() . ')');
            $io->table($headers, $validatorResult->getIncorrectRows());
        }

        if($validatorResult->getSkippedRowsCount()) {
            $io->section('Skipped rows (' . $validatorResult->getSkippedRowsCount() . ')');
            $io->table($headers, $validatorResult->getSkippedRows());
        }

        if ($input->getOption('execute')) {
            try {
                $this->migrate($output);
            } catch (Exception $e) {
                $io->caution('Migration has failed.');

                return Command::FAILURE;
            }
            
            $io->section('Inserting valid data to database...');

            $importResult = $this->dbImport->insert(
                'each', 
                'tblProductData',
                $validatorResult->getValidRows(), [
                    'Product Code' => 'strProductCode',
                    'Product Name' => 'strProductName',
                    'Product Description' => 'strProductDesc',
                    'Stock' => 'intStock',
                    'Cost in GBP' => 'decCost',
                    'Date Added' => 'dtmAdded',
                    'Discontinued' => 'dtmDiscontinued'
                ], [
                    'Product Name',
                    'Product Description',
                    'Stock',
                    'Cost in GBP'
                ]
                , function($row) {
                $row['Date Added'] = new DBRawFunction('NOW()');
                $row['Discontinued'] = $row['Discontinued'] ? new DBRawFunction('NOW()') : null;

                return $row;
            });

            $io->success('Data has been inserted/updated to database');

            if($importResult->getFailedRowsCount()) {
                $io->section('Failed rows (' . $importResult->getFailedRowsCount() . ')');
                $io->table($headers, $importResult->getFailedRows());
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
}
