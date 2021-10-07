<?php

namespace App\Service\CsvImport;

use Symfony\Component\Serializer\Encoder\CsvEncoder;
use Symfony\Component\Serializer\Serializer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;

use Symfony\Component\Filesystem\Exception\FileNotFoundException;

class CsvParser
{
    /**
     * Read CSV and return an array of data rows
     * @param string $filePath path to CSV file
     * @return array CSV rows
     */
    public function serializeFile(string $filePath): array
    {
        if(!file_exists($filePath))
            throw new FileNotFoundException();

        $serializer = new Serializer([new ObjectNormalizer], [new CsvEncoder]);
        
        return $serializer->decode(file_get_contents($filePath), 'csv');
    }
}