package aws

interface TablesProvider {
    fun getConsumableTables(): List<String>;
}