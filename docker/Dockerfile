# PHP 8.5 development/test image for the saga-lara-flow package.
# Provides the extensions Testbench + the package need (pdo_sqlite for the default
# in-memory suite, pdo_mysql for the MySQL run, intl, zip, bcmath, pcntl, mbstring)
# plus Composer.
FROM php:8.5-cli

# Fetch over HTTPS. Some networks sit behind a transparent HTTP cache that hands apt a
# body whose hash does not match the index, and the build dies on "Hash Sum mismatch"
# for a different package each time.
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' \
        /etc/apt/sources.list.d/debian.sources /etc/apt/sources.list 2>/dev/null; \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        unzip \
        libzip-dev \
        libicu-dev \
        libonig-dev \
        libsqlite3-dev \
    && docker-php-ext-install -j"$(nproc)" \
        pdo_sqlite \
        pdo_mysql \
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
