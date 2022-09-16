# tortoise-sentry

## Usage

```python
import sentry_sdk
from tortoise_orm import TortoiseIntegration

sentry_sdk.init(
    dsn=...,
    integrations=[
        TortoiseIntegration(),
    ]
)

```
