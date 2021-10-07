<?php

namespace App\Service\CsvImport\Strategy;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

class DBImportCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container)
    {
        $resolverService = $container->findDefinition(DBImport::class);
        
        $strategyServices = array_keys($container->findTaggedServiceIds(DBImportStrategyInterface::SERVICE_TAG));
 
        foreach ($strategyServices as $strategyService) {
            $resolverService->addMethodCall('addStrategy', [new Reference($strategyService)]);
        }
    }
}