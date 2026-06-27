# PHP 8.5 development/test image for the saga-lara-flow package.
# Provides the extensions Testbench + the package need (pdo_sqlite for in-memory tests,
# intl, zip, bcmath, pcntl, mbstring) plus Composer.
FROM php:8.5-cli

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        unzip \
        libzip-dev \
        libicu-dev \
        libonig-dev \
        libsqlite3-dev \
    && docker-php-ext-install -j"$(nproc)" \
        pdo_sqlite \
        intl \
        zip \
        bcmath \
        pcntl \
        mbstring \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

ENV COMPOSER_ALLOW_SUPERUSER=1

WORKDIR /app
