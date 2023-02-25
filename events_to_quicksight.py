from datashack_glue_sdk import StreamingTable, ProcessTable, QuickSightTable
from datashack_glue_sdk.resource_schemas import Column

UsersMeals = StreamingTable("UsersMeals", "nova_db")
UsersMeals['id'] = Column('string')
UsersMeals['suggested_meal'] = Column('string')
UsersMeals['event_ts'] = Column('timestamp')

UserLogin = StreamingTable("UserLogin", "nova_db")
UserLogin['id'] = Column('string')
UserLogin['login_ts'] = Column("timestamp")

UsersDailyMeals = ProcessTable("UsersDailyMeals", "nova_db")
ProcessTable.composed_process(
    UsersMeals.join(UserLogin,
      on=(UsersMeals.id == UserLogin.id & UsersMeals.event_ts.to_date()==UsersLogin.login_ts.to_date()).groupBy("id",UsersMeals.event_ts.to_date())
        .agg({"number of meals per day": UsersMeals.suggested_meal.count()}))
)

meals_table = QuickSightTable(ingest_from=UsersDailyMeals)