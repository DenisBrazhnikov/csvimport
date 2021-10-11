<?php

namespace App\Tests\Command;

use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

use Symfony\Component\Console\Tester\CommandTester;

use org\bovigo\vfs\vfsStream;
use Symfony\Component\String\ByteString;

class CsvImportTest extends KernelTestcase
{
    private $command;

    protected function setUp():void
    {
        $kernel = static::createKernel();
        $application = new Application($kernel);
        $application->setAutoExit(false);

        // the command should exist
        $this->command = $application->find('app:import');
    }

    public function testItRequiresCsvFile()
    {
        $tester = new CommandTester($this->command);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('missing: "file"');

        $tester->execute([]);
    }

    public function testItFailsWhenFileIsNotFound()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => 'non-existing-file.csv'
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('Can not find CSV file', $output);
    }

    public function testItCanFindCsvFile()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile()
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('Data file has been found', $output);
    }

    public function testItCanReadCsvRecords()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile()
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString($this->countRows($this->genegerateTestCsvData(), true) .' rows detected', $output);
    }

    public function testItFailsOnBrokenCsvFile()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createBrokenCsvFile()
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('CSV file is broken', $output);
    }

    public function testItCanValidateCsvFile()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile()
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('Rows processed: ' . $this->countRows($this->genegerateTestCsvData(), true), $output);
        $this->assertStringContainsString('Valid rows: ' . $this->countRows($this->getValidRows()), $output);
        $this->assertStringContainsString('Incorrect rows: ' . $this->countRows($this->getIncorrectRows()), $output);
        $this->assertStringContainsString('Skipped rows: ' . $this->countRows($this->getRowsThatShouldBeSkipped()), $output);
    }

    public function testItWontSqlInsertWithoutNeed()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile()
        ]);

        $output = $tester->getDisplay();

        $this->assertStringNotContainsString('Migration', $output);
        $this->assertStringNotContainsString('Migrating', $output);
        $this->assertStringNotContainsString('Inserting', $output);
    }

    public function testItCanInsertDataToDatabaseViaDefaultStrategy()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile(),
            '--execute' => true
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('via "batch" strategy', $output);
        $this->assertStringContainsString('Inserting', $output);
        $this->assertStringContainsString('Data has been inserted', $output);
        $this->assertStringNotContainsString('Failed rows', $output);
    }

    public function testItCanInsertDataToDatabaseViaEachStrategy()
    {
        $tester = new CommandTester($this->command);

        $tester->execute([
            'file' => $this->createTestCsvFile(),
            '--execute' => true,
            '--strategy' => 'each'
        ]);

        $output = $tester->getDisplay();

        $this->assertStringContainsString('via "each" strategy', $output);
        $this->assertStringContainsString('Inserting', $output);
        $this->assertStringContainsString('Data has been inserted', $output);
        $this->assertStringNotContainsString('Failed rows', $output);
    }

    private function createTestCsvFile()
    {
        $vfs = vfsStream::setup(sys_get_temp_dir(), null, [
            'test.csv' => $this->genegerateTestCsvData()
        ]);

        return $vfs->url() . '/'. 'test.csv';
    }

    private function genegerateTestCsvData():string
    {
        return 
        $this->getValidHeaderRows() . PHP_EOL .
        $this->getIncorrectRows() . PHP_EOL .
        $this->getRowsThatShouldBeSkipped() . PHP_EOL .
        $this->getValidRows();
    }

    private function getValidHeaderRows(): string
    {
        return implode(',', ['Product Code', 'Product Name', 'Product Description', 'Stock', 'Cost in GBP', 'Discontinued']);
    }

    private function getIncorrectRows():string
    {
        return 
        implode(',', ['', 'Name', 'Description', 10, 399.99, '']) . PHP_EOL . // empty product code
        implode(',', ['Very Long Incorrect Product Code', 'Name', 'Description', 10, 399.99, '']) . PHP_EOL . // too long product code
        implode(',', ['Code', '', 'Description', 10, 399.99, 'yes']) . PHP_EOL . // empty product name
        implode(',', ['Code', ByteString::fromRandom(51), 'Description', 10, 399.99, 'yes']) . PHP_EOL . // too long name
        implode(',', ['Code', 'Name', '', 10, 399.99, 'yes']). PHP_EOL . // empty description
        implode(',', ['Code', 'Name', ByteString::fromRandom(256), 10, 399.99, 'yes']) . PHP_EOL . // too long description
        implode(',', ['Code', 'Name', 'Description', -10, 399.99, 'yes']) . PHP_EOL . // negative stock
        implode(',', ['Code', 'Name', 'Description', 10, -399.99, 'yes']) . PHP_EOL . // negative cost
        implode(',', ['Code', 'Name', 'Description', 10, 399.99, 'discontinued?']); // incorrect discontinued type
    }

    private function getRowsThatShouldBeSkipped():string
    {
        return
        implode(',', ['Code', 'Skipped Product', 'Description', 5, 3, 'yes']) . PHP_EOL . // stock <10 && cost <5$
        implode(',', ['Code', 'Skipped Product', 'Description', 5, 1001, 'yes']); // costs > 1000$
    }

    private function getValidRows(): string
    {
        return
        implode(',', ['Code', 'Name', 'Description', 5, 30.44, 'yes']) . PHP_EOL .
        implode(',', ['Code', 'Name', 'Description', 5, 30.44, '']);
    }

    private function createBrokenCsvFile()
    {
        $vfs = vfsStream::setup(sys_get_temp_dir(), null, [
            'broken.csv' => 'some broken data'
        ]);

        return $vfs->url() . '/'. 'broken.csv';
    }

    /**
     * Count rows of a CSV string
     * @param string $csv CSV string
     * @param bool $header Remove header row
     * @return int Rows count
     */
    private function countRows(string $csv, bool $header = false):int
    {
        return count(explode(PHP_EOL, $csv)) - $header;
    }
}