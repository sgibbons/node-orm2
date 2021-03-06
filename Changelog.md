### v2.0.8 - 8 Apr 2013

- Adds more aggregate functions to the several drivers
- Adds groupBy to aggregate methods (#99)
- Adds possibility to use "-property" to indicate a descending order in Model.find()
- Adds setting instance.returnAllErrors (default: true)
- Changes hasMany.setAccessor to support passing an array of instances (#97)
- Fixes property defaultValue not being set if property is null (closes #104)
- Adds support for indexes on properties that are no associations (#98)
- Adds a new option to add multi-column indexes to models (#98)
- Bug fixes

### v2.0.7 - 3 Apr 2013

- Fixed SQLite driver writing to console when it should not
- Changes Express middleware to wait for connections (errored or not) before processing requests (#92)
- Avoids loosing previously set limit (if set) on Model.fin() (#93)
- Fixes hasMany getAccessor when using an Array as only argument (specific properties)
- Adds ChainFind .last() (similar to .first())
- Fixes hasMany acessor names to correctly convert prop_name to PropName (underscores)
- Adds hasMany hasAcessor conditional to ChainFind (#94)

### v2.0.6 - 22 Mar 2013

- Changes orm.connect to check connection url/opts to avoid throwing some errors about missing protocol or database (#75)
- Hardens some validators againt null/undefined, changes match validator to avoid compiling regex everytime it's called
- Changes back default instance properties to null instead of undefined
- Changes Express middleware to be able to have more than one connection (#76)
- Changes Singleton to avoid cache if save_check option is enabled and cached instance is not saved (#78)
- Adds Model.aggregate()
- Adds 'required' option to hasOne associations
- Changes singleton uid creation to use driver uid (#86)
- Changes Model.drop and Model.sync to be resistive to no callback
- Changes ORM.sync() to also be resistant to no callback
- Many bug fixes

### v2.0.5 - 13 Mar 2013

- Uses sql-query for SQL query building
- Adds initial middleware for Express
- Moves beforeCreate to near beforeSave so people can change instance just like beforeSave (#69)
- Fixes bug when creating Models without all data (#69)
- Changes drivers.count() to be able to pass options (related to #68)
- Changes postgres DDL to create ENUM types before table (#71)
- Changes hasOne.getAccessor to be able to fetch instance before association (if needed)
- Adds support for Object property type in DDL drivers (#72)

### v2.0.4 - 7 Mar 2013

- Changes db.load() to behave like builtin require()
- Moves hook beforeSave to before checking validations (#66)
- Changes postgres driver to support ssl flag and pass it to pg driver
- Adds possibility to add order to hasMany getAccessor (#58)
- Fixes hasOne reversed associations not having setAccessor
- Adds db.ping() (#57)
- Changes db.load to avoid throwing and just create the error
- Added "afterRemove" hook
- Added "afterCreate" hook
- Support Model.find({ prop: null }) (closes #59)
- Adds LIKE operator
- Many bug fixes

### v2.0.3 - 26 Feb 2013

- Fixes postgresql integer columns (#52)
- Adds boolean support for sqlite (#50)
- Fixes an issue where hasMany association properties were not being checked (#49)
- Changes hasMany associations to be able to make some call without callback
- Makes Instances trigger beforeRemove event
- Creates default option for instance.cascadeRemove (true)
- Fixes unique validator not using Model id property name (was using hard coded "id")
- Updated documentation

### v2.0.2 - 21 Feb 2013

- Forces hasMany association changes to check for instance saved (to ensure the instance has an id property)
- Fixes some bugs when not using "id" as instance id property
- Adds default setting instance.cache = true so people can tweak it globally
- Adds autoFetch and autoSave options to default settings
- Adds more documentation about Hooks, fixes Model options list ending

### v2.0.1 - 21 Feb 2013

- Changes singleton to support cache value as a number (timeout in seconds)
- Fixes bug when escaping boolean values in postgres
- Fixes bug on Model.sync (postgres) when property has default value (using wrong path to escape method)
- Fixes DDL drivers creating duplicated field in reversed hasOne associations
- Changes Model instanciation to extend association properties
- Adds support to Amazon Redshift based on PostgreSQL driver

### v2.0.0 - 21 Feb 2013

- Initial release
