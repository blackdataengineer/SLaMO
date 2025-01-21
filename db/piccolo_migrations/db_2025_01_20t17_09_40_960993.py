from piccolo.apps.migrations.auto.migration_manager import MigrationManager


ID = "2025-01-20T17:09:40:960993"
VERSION = "1.22.0"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="", description=DESCRIPTION
    )

    def run():
        print(f"running {ID}")

    manager.add_raw(run)

    return manager
