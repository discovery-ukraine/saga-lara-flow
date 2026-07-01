# Contributing

Contributions are welcome and will be fully credited. Please read and understand this document
before opening an issue or pull request.

## Reporting issues

- Search existing issues first — yours may already be reported.
- Include the package version, PHP/Laravel versions, and a minimal reproduction.
- For security vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Development setup

This is a Laravel **package**, tested against a throwaway app via
[Orchestra Testbench](https://packages.tools/testbench). It requires **PHP `^8.5`**.

If your host PHP differs, run the tooling inside the project's Docker image:

```bash
docker compose run --rm app composer install
docker compose run --rm app composer test
docker compose run --rm app composer lint
```

Or, on a matching PHP toolchain, directly:

```bash
composer install
composer test        # Pest
composer analyse     # PHPStan (larastan, level 5)
composer format      # Laravel Pint
composer lint        # Pint + PHPStan together
```

## Pull requests

- **One feature/fix per PR.** Keep the diff focused.
- **Add tests.** New behaviour needs coverage; the suite runs with random order and fails on risky
  or warning-producing tests.
- **Run `composer lint`** before pushing — CI enforces Pint formatting and PHPStan level 5.
- **Follow existing conventions.** Match the surrounding code's naming, structure, and idioms.
- **Update documentation** (README and the docs under `/docs`) when you change public behaviour.
- **Note queued tests** run against a real database queue driven with `queue:work --stop-when-empty`,
  not the `sync` driver.

## Running a single test

```bash
vendor/bin/pest tests/Feature/CreateWorkflowTest.php
vendor/bin/pest --filter="cancels a non-terminal run"
```

**Happy coding!**
