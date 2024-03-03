package utilities

import org.apache.kafka.connect.errors.ConnectException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import utilities.AvroSchemaNameParser.ReplacementOccurred
import java.util.*
import java.util.concurrent.ConcurrentHashMap


/**
 * A adjuster for the names of change data message schemas. Currently, this solely implements the rules required for
 * using these schemas in Avro messages. Avro [rules for
 * schema fullnames](http://avro.apache.org/docs/current/spec.html#names) are as follows:
 *
 *  * Each has a fullname that is composed of two parts; a name and a namespace. Equality of names is defined on the
 * fullname.
 *  * The name portion of a fullname, record field names, and enum symbols must start with a Latin letter or underscore
 * character (e.g., [A-Z,a-z,_]); and subsequent characters must be Latin alphanumeric or the underscore (`_`)
 * characters (e.g., [A-Z,a-z,0-9,_]).
 *  * A namespace is a dot-separated sequence of such names.
 *  * Equality of names (including field names and enum symbols) as well as fullnames is case-sensitive.
 *
 *
 *
 * A `com.trustpilot.connector.dynamodb.com.trustpilot.connector.dynamodb.utils.SchemaNameAdjuster` can determine if the supplied fullname follows these Avro rules.
 *
 * @author Randall Hauch
 */
fun interface AvroSchemaNameParser {
    /**
     * Convert the proposed string to a valid Avro fullname, replacing all invalid characters with the underscore ('_')
     * character.
     *
     * @param proposedName the proposed fullname; may not be null
     * @return the valid fullname for Avro; never null
     */
    fun adjust(proposedName: String?): String?

    /**
     * Function used to determine the replacement for a character that is not valid per Avro rules.
     */
    fun interface ReplacementFunction {
        /**
         * Determine the replacement string for the invalid character.
         *
         * @param invalid the invalid character
         * @return the replacement string; may not be null
         */
        fun replace(invalid: Char): String?
    }

    /**
     * Function used to report that an original value was replaced with an Avro-compatible string.
     */
    fun interface ReplacementOccurred {
        /**
         * Accept that the original value was not Avro-compatible and was replaced.
         *
         * @param original the original value
         * @param replacement the replacement value
         * @param conflictsWithOriginal the other original value that resulted in the same replacement; may be null if there is
         * no conflict
         */
        fun accept(original: String?, replacement: String?, conflictsWithOriginal: String?)

        /**
         * Create a new function that calls this function only the first time it sees each unique original, and ignores
         * subsequent calls for originals it has already seen.
         *
         * @return the new function; never null
         */
        fun firstTimeOnly(): ReplacementOccurred? {
            val delegate = this
            val alreadySeen = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
            val originalByReplacement: MutableMap<String, String> = ConcurrentHashMap()
            return ReplacementOccurred { original: String?, replacement: String?, conflictsWith: String? ->
                if (alreadySeen.add(original)) {
                    // We've not yet seen this original ...
                    val replacementsOriginal = original?.let { originalByReplacement.put(replacement ?: "", it) }
                    if (replacementsOriginal == null || original == replacementsOriginal) {
                        // We've not seen the replacement yet, so handle it ...
                        delegate.accept(original, replacement, null)
                    } else {
                        // We've already seen the replacement with a different original, so this is a conflict ...
                        delegate.accept(original, replacement, replacementsOriginal)
                    }
                }
            }
        }

        /**
         * Create a new function that calls this function and then calls the next function.
         *
         * @param next the function to call after this function; may be null
         * @return the new function; never null
         */
        fun andThen(next: ReplacementOccurred?): ReplacementOccurred? {
            return if (next == null) {
                this
            } else ReplacementOccurred { original: String?, replacement: String?, conflictsWith: String? ->
                accept(original, replacement, conflictsWith)
                next.accept(original, replacement, conflictsWith)
            }
        }
    }

    companion object {
        val DEFAULT = create(LoggerFactory.getLogger(AvroSchemaNameParser::class.java))

        /**
         * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
         * with a valid fullname and throws an exception. This method replaces all invalid characters with the underscore character
         * ('_').
         *
         * @return the validator; never null
         */
        fun defaultAdjuster(): AvroSchemaNameParser {
            return DEFAULT
        }
        /**
         * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
         * with a valid fullname. This method replaces all invalid characters with the underscore character ('_').
         *
         * @param logger the logger to use; may not be null
         * @param uponConflict the function to be called when there is a conflict and after that conflict is logged; may be null
         * @return the validator; never null
         */
        /**
         * Create a stateful Avro fullname adjuster that logs a warning the first time an invalid fullname is seen and replaced
         * with a valid fullname, and throws an error if the replacement conflicts with that of a different original. This method
         * replaces all invalid characters with the underscore character ('_').
         *
         * @param logger the logger to use; may not be null
         * @return the validator; never null
         */
        @JvmOverloads
        fun create(
            logger: Logger,
            uponConflict: ReplacementOccurred? = ReplacementOccurred { original: String?, replacement: String?, conflict: String? ->
                val msg = "The Kafka Connect schema name '" + original +
                        "' is not a valid Avro schema name and its replacement '" + replacement +
                        "' conflicts with another different schema '" + conflict + "'"
                throw ConnectException(msg)
            },
        ): AvroSchemaNameParser {
            val handler =
                ReplacementOccurred { original: String?, replacement: String?, conflictsWith: String? ->
                    if (conflictsWith != null) {
                        logger.error(
                            "The Kafka Connect schema name '{}' is not a valid Avro schema name and its replacement '{}' conflicts with another different schema '{}'",
                            original, replacement, conflictsWith
                        )
                        uponConflict?.accept(original, replacement, conflictsWith)
                    } else {
                        logger.warn(
                            "The Kafka Connect schema name '{}' is not a valid Avro schema name, so replacing with '{}'",
                            original,
                            replacement
                        )
                    }
                }
            return create(handler.firstTimeOnly())
        }

        fun create(uponReplacement: ReplacementOccurred?): AvroSchemaNameParser {
            return create("_", uponReplacement)
        }

        fun create(replacement: Char, uponReplacement: ReplacementOccurred?): AvroSchemaNameParser {
            val replacementStr = "" + replacement
            return AvroSchemaNameParser { original: String? ->
                validFullname(
                    original,
                    { c: Char -> replacementStr }, uponReplacement
                )
            }
        }

        fun create(replacement: String?, uponReplacement: ReplacementOccurred?): AvroSchemaNameParser {
            return AvroSchemaNameParser { original: String? ->
                validFullname(
                    original,
                    { c: Char -> replacement }, uponReplacement
                )
            }
        }

        fun create(function: ReplacementFunction, uponReplacement: ReplacementOccurred?): AvroSchemaNameParser? {
            return AvroSchemaNameParser { original: String? ->
                validFullname(
                    original,
                    function,
                    uponReplacement
                )
            }
        }

        fun isValidFullname(fullname: String): Boolean {
            if (fullname.isEmpty()) {
                return true
            }
            var c = fullname[0]
            if (!isValidFullnameFirstCharacter(c)) {
                return false
            }
            for (i in 1..<fullname.length) {
                c = fullname[i]
                if (!isValidFullnameNonFirstCharacter(c)) {
                    return false
                }
            }
            return true
        }

        private fun isValidFullnameFirstCharacter(c: Char): Boolean {
            return c == '_' || c in 'A'..'Z' || c in 'a'..'z'
        }

        private fun isValidFullnameNonFirstCharacter(c: Char): Boolean {
            return c == '.' || isValidFullnameFirstCharacter(c) || c in '0'..'9'
        }

        @JvmOverloads
        fun validFullname(proposedName: String?, replacement: String? = "_"): String? {
            return validFullname(proposedName, { c: Char -> replacement })
        }

        @JvmOverloads
        fun validFullname(
            proposedName: String?,
            replacement: ReplacementFunction,
            uponReplacement: ReplacementOccurred? = null,
        ): String? {
            if (proposedName.isNullOrEmpty()) {
                return proposedName
            }
            val sb = StringBuilder()
            var c = proposedName[0]
            var changed = false
            if (isValidFullnameFirstCharacter(c)) {
                sb.append(c)
            } else {
                sb.append(replacement.replace(c))
                changed = true
            }
            for (i in 1..<proposedName.length) {
                c = proposedName[i]
                if (isValidFullnameNonFirstCharacter(c)) {
                    sb.append(c)
                } else {
                    sb.append(replacement.replace(c))
                    changed = true
                }
            }
            if (!changed) {
                return proposedName
            }
            val result = sb.toString()
            uponReplacement?.accept(proposedName, result, null)
            return result
        }
    }
}