class Envelope {
    enum class Operation(val code: String) {
        Read("r"),
        Create("c"),
        Update("u"),
        Delete("d")
    }

    fun forCode(code: String): Operation {
        return Operation.valueOf(code);
    }


    object FieldName {
        const val Version = "version"

        const val Document = "document"

        const val Operation = "op"

        const val Source = "source"

        const val Timestamp = "ts_ms"
    }


}