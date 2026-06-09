#!/usr/bin/env php
<?php

function supportsAnsi(): bool
{
    if (getenv('NO_COLOR') !== false) {
        return false;
    }

    if (PHP_OS_FAMILY === 'Windows') {
        return (function_exists('sapi_windows_vt100_support')
            && sapi_windows_vt100_support(STDOUT))
            || getenv('ANSICON') !== false
            || getenv('ConEmuANSI') === 'ON'
            || str_starts_with((string) getenv('TERM'), 'xterm');
    }

    return stream_isatty(STDOUT);
}

function ansi(string $text, string $code): string
{
    if (! supportsAnsi()) {
        return $text;
    }

    return "\033[{$code}m{$text}\033[0m";
}

function bold(string $text): string
{
    return ansi($text, '1');
}

function dim(string $text): string
{
    return ansi($text, '2');
}

function green(string $text): string
{
    return ansi($text, '32');
}

function yellow(string $text): string
{
    return ansi($text, '33');
}

function writeln(string $line): void
{
    echo $line.PHP_EOL;
}

function ask(string $question, string $default = ''): string
{
    $prompt = bold($question);

    if ($default) {
        $prompt .= ' '.dim("({$default})");
    }

    $answer = readline('  '.$prompt.': ');

    if (! $answer) {
        return $default;
    }

    return $answer;
}

function confirm(string $question, bool $default = false): bool
{
    $answer = ask($question.' '.($default ? 'Y/n' : 'y/N'));

    if (! $answer) {
        return $default;
    }

    return strtolower($answer) === 'y';
}

function run(string $command): string
{
    return trim((string) shell_exec($command));
}

function str_after(string $subject, string $search): string
{
    $pos = strrpos($subject, $search);

    if ($pos === false) {
        return $subject;
    }

    return substr($subject, $pos + strlen($search));
}

function slugify(string $subject): string
{
    return strtolower(trim(preg_replace('/[^A-Za-z0-9-]+/', '-', $subject), '-'));
}

function title_case(string $subject): string
{
    return str_replace(' ', '', ucwords(str_replace(['-', '_'], ' ', $subject)));
}

function title_snake(string $subject, string $replace = '_'): string
{
    return str_replace(['-', '_'], $replace, $subject);
}

function replace_in_file(string $file, array $replacements): void
{
    $contents = file_get_contents($file);

    file_put_contents(
        $file,
        str_replace(
            array_keys($replacements),
            array_values($replacements),
            $contents
        )
    );
}

function remove_prefix(string $prefix, string $content): string
{
    if (str_starts_with($content, $prefix)) {
        return substr($content, strlen($prefix));
    }

    return $content;
}

function remove_readme_paragraphs(string $file): void
{
    $contents = file_get_contents($file);

    file_put_contents(
        $file,
        preg_replace('/<!--delete-->.*<!--\/delete-->/s', '', $contents) ?: $contents
    );
}

function safeUnlink(string $filename): void
{
    if (file_exists($filename) && is_file($filename)) {
        unlink($filename);
    }
}

function normalizePath(string $path): string
{
    return str_replace('/', DIRECTORY_SEPARATOR, $path);
}

function getFilesWithPlaceholders(): array
{
    $directory = new RecursiveDirectoryIterator(__DIR__, RecursiveDirectoryIterator::SKIP_DOTS);
    $iterator = new RecursiveIteratorIterator($directory);

    $skipDirs = ['.git', 'vendor', 'node_modules'];
    $scriptBasename = basename(__FILE__);
    $placeholders = [':author', ':vendor', ':package', 'VendorName', 'skeleton', 'Skeleton', 'migration_table_name', 'vendor_name', 'vendor_slug', 'author@domain.com', ':variable'];

    $files = [];

    foreach ($iterator as $file) {
        if (! $file->isFile()) {
            continue;
        }

        $path = $file->getPathname();
        $relativePath = str_replace(__DIR__.DIRECTORY_SEPARATOR, '', $path);

        // Skip excluded directories
        foreach ($skipDirs as $skipDir) {
            if (str_starts_with($relativePath, $skipDir.DIRECTORY_SEPARATOR)) {
                continue 2;
            }
        }

        // Skip this script
        if ($file->getBasename() === $scriptBasename) {
            continue;
        }

        // Check if file contains any placeholders
        $contents = file_get_contents($path);
        foreach ($placeholders as $placeholder) {
            if (stripos($contents, $placeholder) !== false) {
                $files[] = $path;
                break;
            }
        }
    }

    return $files;
}

function modifyComposerJson(array $removeDeps, array $removeScripts): void
{
    $path = __DIR__.'/composer.json';
    $data = json_decode(file_get_contents($path), true);

    foreach ($removeDeps as $name) {
        unset($data['require-dev'][$name]);
    }

    foreach ($removeScripts as $name) {
        unset($data['scripts'][$name]);
    }

    file_put_contents($path, json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
}

function getGitHubApiEndpoint(string $endpoint): ?stdClass
{
    try {
        $curl = curl_init("https://api.github.com/{$endpoint}");
        curl_setopt_array($curl, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_HTTPGET => true,
            CURLOPT_HTTPHEADER => [
                'User-Agent: spatie-configure-script/1.0',
            ],
        ]);

        $response = curl_exec($curl);
        $statusCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);

        if ($statusCode === 200) {
            return json_decode($response);
        }
    } catch (Exception $e) {
        // ignore
    }

    return null;
}

function searchCommitsForGitHubUsername(): string
{
    $authorName = strtolower(trim(shell_exec('git config user.name')));

    $committersRaw = shell_exec("git log --author='@users.noreply.github.com' --pretty='%an:%ae' --reverse");
    $committersLines = explode("\n", $committersRaw ?? '');
    $committers = array_filter(array_map(function ($line) use ($authorName) {
        $line = trim($line);
        [$name, $email] = explode(':', $line) + [null, null];

        return [
            'name' => $name,
            'email' => $email,
            'isMatch' => strtolower($name) === $authorName && ! str_contains($name, '[bot]'),
        ];
    }, $committersLines), fn ($item) => $item['isMatch']);

    if (empty($committers)) {
        return '';
    }

    $firstCommitter = reset($committers);

    return explode('@', $firstCommitter['email'])[0] ?? '';
}

function guessGitHubUsernameUsingCli()
{
    try {
        if (preg_match('/ogged in to github\.com as ([a-zA-Z-_]+).+/', shell_exec('gh auth status -h github.com 2>&1'), $matches)) {
            return $matches[1];
        }
    } catch (Exception $e) {
        // ignore
    }

    return '';
}

function guessGitHubUsername(): string
{
    $username = searchCommitsForGitHubUsername();
    if (! empty($username)) {
        return $username;
    }

    $username = guessGitHubUsernameUsingCli();
    if (! empty($username)) {
        return $username;
    }

    // fall back to using the username from the git remote
    $remoteUrl = shell_exec('git config remote.origin.url') ?? '';
    $remoteUrlParts = explode('/', str_replace(':', '/', trim($remoteUrl)));

    return $remoteUrlParts[1] ?? '';
}

function guessGitHubVendorInfo($authorName, $username): array
{
    $remoteUrl = shell_exec('git config remote.origin.url') ?? '';
    $remoteUrlParts = explode('/', str_replace(':', '/', trim($remoteUrl)));

    if (! isset($remoteUrlParts[1])) {
        return [$authorName, $username];
    }

    $response = getGitHubApiEndpoint("orgs/{$remoteUrlParts[1]}");

    if ($response === null) {
        return [$authorName, $username];
    }

    return [$response->name ?? $authorName, $response->login ?? $username];
}

writeln('');

$logoLines = [
    '  ███████ ██████   █████  ████████ ████ ████████',
    '  ██      ██   ██ ██   ██    ██     ██  ██',
    '  ███████ ██████  ███████    ██     ██  ██████',
    '       ██ ██      ██   ██    ██     ██  ██',
    '  ███████ ██      ██   ██    ██    ████ ████████',
];

$gradientColors = [
    '38;2;100;200;225',
    '38;2;62;170;200',
    '38;2;35;140;175',
    '38;2;25;117;147',
    '38;2;15;90;115',
];

foreach ($logoLines as $i => $line) {
    writeln(supportsAnsi() ? "\033[{$gradientColors[$i]}m{$line}\033[0m" : $line);
}

writeln('');

if (supportsAnsi()) {
    writeln("  \033[48;2;25;117;147m\033[97m ✦ Laravel Package Skeleton :: spatie.be ✦ \033[0m");
} else {
    writeln('  ✦ Laravel Package Skeleton :: spatie.be ✦');
}

writeln('');
writeln('  Thanks for using the Spatie Laravel package skeleton!');
writeln('  Let\'s get your new package configured.');
writeln('');

writeln(bold('  Author'));
writeln(dim('  Used for composer.json credits and the README.'));
writeln('');

$gitName = run('git config user.name');
$authorName = ask('Author name', $gitName);

$gitEmail = run('git config user.email');
$authorEmail = ask('Author email', $gitEmail);
$authorUsername = ask('Author username', guessGitHubUsername());

writeln('');
writeln(bold('  Vendor'));
writeln(dim('  The vendor is your brand on Packagist, e.g. in spatie/laravel-ray the vendor is "spatie".'));
writeln('');

$guessGitHubVendorInfo = guessGitHubVendorInfo($authorName, $authorUsername);

$vendorName = ask('Vendor name', $guessGitHubVendorInfo[0]);
writeln('');
writeln(dim('  The slug used in composer.json, e.g. "spatie" in spatie/laravel-ray.'));
$vendorUsername = ask('Vendor username', $guessGitHubVendorInfo[1] ?? slugify($vendorName));
$vendorSlug = slugify($vendorUsername);
writeln('');
$vendorNamespace = str_replace('-', '', ucwords($vendorName));
writeln(dim('  The PHP namespace prefix for your package, e.g. Spatie\\LaravelRay.'));
$vendorNamespace = ask('Vendor namespace', $vendorNamespace);

writeln('');
writeln(bold('  Package'));
writeln('');

$currentDirectory = getcwd();
$folderName = basename($currentDirectory);

$packageName = ask('Package name', $folderName);
$packageSlug = slugify($packageName);
$packageSlugWithoutPrefix = remove_prefix('laravel-', $packageSlug);

$className = title_case($packageName);
$className = ask('Class name', $className);
$variableName = lcfirst($className);
$description = ask('Package description', "This is my package {$packageSlug}");

writeln('');
writeln(bold('  Tooling'));
writeln(dim('  Choose which dev tools to include. You can always add these later.'));
writeln('');

writeln(dim('  Static analysis to catch bugs before they reach production.'));
$usePhpStan = confirm('Enable PhpStan?', true);
writeln('');
writeln(dim('  Automatic code formatting following Laravel conventions.'));
$useLaravelPint = confirm('Enable Laravel Pint?', true);
writeln('');
writeln(dim('  Automated dependency update PRs via GitHub.'));
$useDependabot = confirm('Enable Dependabot?', true);
writeln('');
writeln(dim('  Debug your package with the Ray desktop app.'));
$useLaravelRay = confirm('Use Ray for debugging?', true);
writeln('');
writeln(dim('  Automatically updates CHANGELOG.md when a new release is tagged.'));
$useUpdateChangelogWorkflow = confirm('Use automatic changelog updater workflow?', true);

writeln('');
writeln(bold('  Summary'));
writeln('');
writeln("  Author      {$authorName} ({$authorUsername}, {$authorEmail})");
writeln("  Vendor      {$vendorName} ({$vendorSlug})");
writeln("  Package     {$packageSlug}");
writeln("  Description {$description}");
writeln("  Namespace   {$vendorNamespace}\\{$className}");
writeln("  Class       {$className}");
writeln('');
writeln('  Tooling');
writeln('  PhpStan          '.($usePhpStan ? green('yes') : dim('no')));
writeln('  Laravel Pint     '.($useLaravelPint ? green('yes') : dim('no')));
writeln('  Dependabot       '.($useDependabot ? green('yes') : dim('no')));
writeln('  Ray              '.($useLaravelRay ? green('yes') : dim('no')));
writeln('  Auto-Changelog   '.($useUpdateChangelogWorkflow ? green('yes') : dim('no')));
writeln('');

if (! confirm('Modify files?', true)) {
    exit(1);
}

writeln('');

$files = getFilesWithPlaceholders();

foreach ($files as $file) {
    replace_in_file($file, [
        ':author_name' => $authorName,
        ':author_username' => $authorUsername,
        'author@domain.com' => $authorEmail,
        ':vendor_name' => $vendorName,
        ':vendor_slug' => $vendorSlug,
        'VendorName' => $vendorNamespace,
        ':package_name' => $packageName,
        ':package_slug' => $packageSlug,
        'Skeleton' => $className,
        'skeleton' => $packageSlug,
        'migration_table_name' => title_snake($packageSlug),
        ':variable' => $variableName,
        ':package_description' => $description,
    ]);

    match (true) {
        str_contains($file, normalizePath('src/Skeleton.php')) => rename($file, normalizePath('./src/'.$className.'.php')),
        str_contains($file, normalizePath('src/SkeletonServiceProvider.php')) => rename($file, normalizePath('./src/'.$className.'ServiceProvider.php')),
        str_contains($file, normalizePath('src/Facades/Skeleton.php')) => rename($file, normalizePath('./src/Facades/'.$className.'.php')),
        str_contains($file, normalizePath('src/Commands/SkeletonCommand.php')) => rename($file, normalizePath('./src/Commands/'.$className.'Command.php')),
        str_contains($file, normalizePath('database/migrations/create_skeleton_table.php.stub')) => rename($file, normalizePath('./database/migrations/create_'.title_snake($packageSlugWithoutPrefix).'_table.php.stub')),
        str_contains($file, normalizePath('config/skeleton.php')) => rename($file, normalizePath('./config/'.$packageSlugWithoutPrefix.'.php')),
        str_contains($file, 'README.md') => remove_readme_paragraphs($file),
        default => null,
    };
}

writeln(green('  ✓ Updated '.count($files).' files'));

if (! $useLaravelPint) {
    safeUnlink(__DIR__.'/.github/workflows/fix-php-code-style-issues.yml');
    safeUnlink(__DIR__.'/pint.json');
    writeln(green('  ✓ Removed Laravel Pint configuration'));
}

if (! $usePhpStan) {
    safeUnlink(__DIR__.'/phpstan.neon.dist');
    safeUnlink(__DIR__.'/phpstan-baseline.neon');
    safeUnlink(__DIR__.'/.github/workflows/phpstan.yml');
    writeln(green('  ✓ Removed PhpStan configuration'));
}

if (! $useDependabot) {
    safeUnlink(__DIR__.'/.github/dependabot.yml');
    safeUnlink(__DIR__.'/.github/workflows/dependabot-auto-merge.yml');
    writeln(green('  ✓ Removed Dependabot configuration'));
}

if (! $useUpdateChangelogWorkflow) {
    safeUnlink(__DIR__.'/.github/workflows/update-changelog.yml');
    writeln(green('  ✓ Removed changelog updater workflow'));
}

// Batch all composer.json modifications
$removeDeps = [];
$removeScripts = [];

if (! $usePhpStan) {
    $removeDeps = array_merge($removeDeps, [
        'phpstan/extension-installer',
        'phpstan/phpstan-deprecation-rules',
        'phpstan/phpstan-phpunit',
        'larastan/larastan',
    ]);
    $removeScripts[] = 'analyse';
}

if (! $useLaravelRay) {
    $removeDeps[] = 'spatie/laravel-ray';
}

if (! empty($removeDeps) || ! empty($removeScripts)) {
    modifyComposerJson($removeDeps, $removeScripts);
    writeln(green('  ✓ Cleaned up composer.json'));
}

confirm('Execute `composer install` and run tests?', true) && run('composer install && composer test');

writeln('');
confirm('Let this script delete itself?', true) && unlink(__FILE__);

writeln('');
writeln(green(bold('  ✨ You\'re all set! Happy building!')));
writeln(dim('  Need help creating a package? Check out https://laravelpackage.training'));
writeln('');
