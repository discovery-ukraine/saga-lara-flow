# Simple workflow management engine with an integrated Saga pattern using Laravel Queues.

[![Latest Version on Packagist](https://img.shields.io/packagist/v/discovery-ukraine/saga-lara-flow.svg?style=flat-square)](https://packagist.org/packages/discovery-ukraine/saga-lara-flow)
[![Total Downloads](https://img.shields.io/packagist/dt/discovery-ukraine/saga-lara-flow.svg?style=flat-square)](https://packagist.org/packages/discovery-ukraine/saga-lara-flow)

This package will help you to manage your workflow with an integrated Saga pattern using Laravel Queues.
It is inspired by great [Durable Workflow (formerly Laravel Workflow)](https://github.com/durable-workflow/workflow) package, but it is not a replacement for it.
It is a simple and lightweight alternative.

## Installation

You can install the package via composer:

```bash
composer require discovery-ukraine/saga-lara-flow
```

You can publish and run the migrations with:

```bash
php artisan vendor:publish --tag="saga-lara-flow-migrations"
php artisan migrate
```

You can publish the config file with:

```bash
php artisan vendor:publish --tag="saga-lara-flow-config"
```

This is the contents of the published config file:

```php
return [
];
```

Optionally, you can publish the views using

```bash
php artisan vendor:publish --tag="saga-lara-flow-views"
```

## Usage

```php
// Will be available in the future
```

## Testing

```bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Credits

- [Andriy Karpishyn](https://github.com/discovery-ukraine)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
