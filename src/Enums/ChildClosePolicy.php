<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum ChildClosePolicy: string
{
    case Abandon = 'abandon';
    case Cancel = 'cancel';
    case Fail = 'fail';
}
