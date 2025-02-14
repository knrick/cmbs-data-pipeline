from pyspark.sql.types import *
from pyspark.sql import functions as F
import json
import os
from datetime import datetime
import re
from .logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger(__name__)

class SchemaManager:
    """Manages schema versioning and validation for CMBS data."""
    
    def __init__(self, schema_dir="./schemas"):
        """Initialize SchemaManager with schema directory."""
        logger.info("Initializing SchemaManager")
        self.schema_dir = schema_dir
        os.makedirs(schema_dir, exist_ok=True)
        
        # Create subdirectories for each schema type
        self.loan_schema_dir = os.path.join(schema_dir, "loan")
        self.property_schema_dir = os.path.join(schema_dir, "property")
        os.makedirs(self.loan_schema_dir, exist_ok=True)
        os.makedirs(self.property_schema_dir, exist_ok=True)
        logger.info(f"Created schema directories in {schema_dir}")
        
        # Load both schemas
        logger.info("Loading schemas")
        self.loan_schema, self.loan_schema_file = self._load_current_schema("loan", "loan_schema.json")
        self.property_schema, self.property_schema_file = self._load_current_schema("property", "property_schema.json")
        logger.info("Schemas loaded successfully")
        
        # Enhanced type mapping with size considerations
        self.type_mapping = {
            # String types
            "string": StringType(),
            "char": StringType(),
            
            # Integer types
            "Int8": ByteType(),      # -128 to 127
            "Int16": ShortType(),    # -32,768 to 32,767
            "Int32": IntegerType(),  # -2^31 to 2^31-1
            "Int64": LongType(),     # -2^63 to 2^63-1
            
            # Unsigned integer types (mapped to next larger signed type)
            "UInt8": ShortType(),    # 0 to 255
            "UInt16": IntegerType(), # 0 to 65,535
            "UInt32": LongType(),    # 0 to 4,294,967,295
            
            # Floating point types
            "Float32": FloatType(),  # Single precision
            "Float64": DoubleType(), # Double precision
            
            # Date and time types
            "datetime": TimestampType(),
            "date": DateType(),
            
            # Boolean type
            "bool": BooleanType(),
            
            # Binary type
            "binary": BinaryType()
        }
        
        # Decimal type cache to avoid recreating the same types
        self._decimal_type_cache = {}
        
    def _get_schema_file(self, schema):
        """Get the file path for the schema type."""
        schema_dir = self.loan_schema_dir if schema["type"] == "loan" else self.property_schema_dir
        return os.path.join(schema_dir, f"{schema['type']}_schema_v{schema['version']}.json")
    
    def _load_current_schema(self, schema_type, base_schema_file):
        """Load the most recent schema version for the specified type."""
        logger.info(f"Loading current schema for {schema_type}")
        schema_dir = self.loan_schema_dir if schema_type == "loan" else self.property_schema_dir
        schema_files = sorted([f for f in os.listdir(schema_dir) 
                             if f.startswith(f"{schema_type}_schema_v") and f.endswith(".json")])
        
        if not schema_files:
            logger.info(f"No existing schema files found for {schema_type}, loading base schema")
            # Load base schema from file
            base_schema_path = os.path.join(os.path.dirname(__file__), base_schema_file)
            if not os.path.exists(base_schema_path):
                logger.error(f"Base schema file not found: {base_schema_path}")
                raise FileNotFoundError(f"Base schema file not found: {base_schema_path}")
                
            with open(base_schema_path, 'r') as f:
                base_schema = json.load(f)
                base_schema["type"] = schema_type
                base_schema["version"] = 1
                base_schema["effective_date"] = datetime.now().isoformat()
            
            filepath = self._save_schema(base_schema)
            logger.info(f"Created initial schema version for {schema_type}")
            return base_schema, filepath
            
        latest_schema_file = schema_files[-1]
        logger.info(f"Loading schema from {latest_schema_file}")
        filepath = os.path.join(schema_dir, latest_schema_file)
        with open(filepath, 'r') as f:
            return json.load(f), filepath
    
    def _save_schema(self, schema):
        """Save schema to file."""
        try:
            version = schema["version"]
            schema_type = schema["type"]
            # Add metadata
            schema["effective_date"] = datetime.now().isoformat()
            
            # Save to file
            filepath = self._get_schema_file(schema)
            
            with open(filepath, "w") as f:
                json.dump(schema, f, indent=2)
                
            logger.info(f"Saving schema version {version} for {schema_type}")
            logger.info(f"Schema saved to {filepath}")

            return filepath
            
        except Exception as e:
            logger.error(f"Error saving schema: {str(e)}")
            raise
    
    def update_schema(self, schema, replace=False):
        """Update schema with new field specifications.
        
        Args:
            schema_type: Type of schema to update ('loan' or 'property')
            field_specs: Dictionary of field specifications to update
        """
        logger.info(f"Updating {schema['type']} schema")
        
        # Create a new schema with the provided field specs
        schema["version"] = self._get_next_version(schema["type"])
        
        # Replace the existing schema
        if replace:
            if schema["type"] == "loan":
                self.loan_schema = schema
            else:
                self.property_schema = schema
            
        # Save updated schema
        self._save_schema(schema)
    
    def get_schema_history(self, schema_type):
        """Get the history of schema versions."""
        logger.info(f"Getting schema history for {schema_type}")
        schema_dir = self.loan_schema_dir if schema_type == "loan" else self.property_schema_dir
        schema_files = sorted([f for f in os.listdir(schema_dir) 
                             if f.startswith(f"{schema_type}_schema_v") and f.endswith(".json")])
        
        history = []
        for schema_file in schema_files:
            logger.info(f"Reading schema file: {schema_file}")
            with open(os.path.join(schema_dir, schema_file), 'r') as f:
                schema = json.load(f)
                history.append({
                    "version": schema.get("version", 1),
                    "effective_date": schema.get("effective_date"),
                    "columns": list(schema.get("columns", {}).keys())
                })
        
        return history
    
    def get_spark_schema(self, schema_type="loan"):
        """Convert the specified schema to a Spark StructType."""
        logger.info(f"Converting {schema_type} schema to Spark StructType")
        schema = self.loan_schema if schema_type == "loan" else self.property_schema
        fields = []
        
        for col_name, col_spec in schema["columns"].items():
            spark_type = self._get_spark_type(col_spec["type"])
            fields.append(
                StructField(col_name, spark_type, col_spec["nullable"])
            )
        
        return StructType(fields)
    
    def validate_dataframe_schema(self, df, validation_configs):
        """Validate DataFrame schema and data types.

        Args:
            df: The DataFrame to validate
            validation_configs: String schema type ('loan' or 'property') or list of schema types

        Returns:
            dict: Dictionary with keys 'loan' and/or 'property' containing the validated DataFrames
        """
        logger.info("Validating DataFrame against schema")

        try:
            # Convert single string to list for uniform handling
            if isinstance(validation_configs, str):
                validation_configs = [validation_configs]
            elif not isinstance(validation_configs, list):
                raise ValueError("validation_configs must be a string schema type or list of schema types")

            # Use dictionary to store field specs with column as key
            all_field_specs = {}
            
            # Track columns for each table type
            table_columns = {"loan": set(), "property": set()}
            
            for config in validation_configs:
                if not isinstance(config, str) or config not in ["loan", "property"]:
                    raise ValueError("Each validation config must be either 'loan' or 'property'")
                    
                schema = self.loan_schema if config == "loan" else self.property_schema
                for col, new_schema in schema["columns"].items():
                    if col in all_field_specs:
                        # Assert schema matches for shared columns
                        if all_field_specs[col]["schema"] != new_schema:
                            raise ValueError(
                                f"Schema mismatch for column {col} between {all_field_specs[col]['table']} "
                                f"and {config} tables. Schemas must be identical for shared columns."
                            )
                        # Update table type to "both"
                        all_field_specs[col]["table"] = "both"
                        # Add column to both table types
                        table_columns["loan"].add(col)
                        table_columns["property"].add(col)
                    else:
                        all_field_specs[col] = {
                            "table": config,
                            "schema": new_schema
                        }
                        # Add column to its table type
                        table_columns[config].add(col)

            # Validate the full DataFrame first
            validated_df = self._validate_field_specs(df, all_field_specs)
            
            # Split and select columns for each table type
            result = {}
            for config in validation_configs:
                # Get relevant columns for this table (including _validation_type)
                columns = list(table_columns[config]) + ["_validation_type"]
                
                # Filter rows for this table type and select only relevant columns
                table_df = validated_df.filter(F.col("_validation_type") == config).select(*columns)
                result[config] = table_df
            
            return result

        except Exception as e:
            logger.error(f"Error during DataFrame validation: {str(e)}")
            raise
    
    def _validate_field_specs(self, df, field_specs):
        """Validate DataFrame against field specifications."""
        # Check for columns in DataFrame that aren't in the schema
        schema_columns = set(field_specs)
        unknown_columns = {col for col in df.columns if col != "_validation_type" and col not in schema_columns}
        if unknown_columns:
            raise ValueError(f"Found columns in DataFrame that are not defined in schema: {unknown_columns}")
        
        # Get the intersection of DataFrame columns and field specs
        columns_to_validate = set(df.columns).intersection(schema_columns)
        
        # First pass: Validate all fields that exist in the DataFrame
        validation_exprs = []
        
        for field_name, field_spec in field_specs.items():
            if field_name not in columns_to_validate:
                continue
            validation_table = field_spec["table"]
            spec = field_spec["schema"]
            field_type = spec["type"]
            nullable = spec.get("nullable", True)
            
            # Get Spark type for casting
            spark_type = self._get_spark_type(field_type)
            
            # String validations
            if isinstance(spark_type, StringType):
                # Check for control characters
                validation_exprs.append(
                    F.when(F.col(field_name).rlike("[\x00-\x1F\x7F]"), 
                          F.lit(f"invalid string values in {field_name}"))
                )
                # Check for empty strings in non-nullable fields
                if not nullable:
                    if validation_table == "both":
                        validation_exprs.append(
                            F.when(
                                (F.trim(F.col(field_name)) == ""),
                                F.lit(f"empty string values in {field_name}")
                            )
                        )
                    else:
                        validation_exprs.append(
                            F.when(
                                (F.col("_validation_type") == validation_table) &
                                (F.trim(F.col(field_name)) == ""),
                                F.lit(f"empty string values in {field_name}")
                            )
                        )
            
            # Date validations
            elif isinstance(spark_type, DateType):
                # First check for valid date
                validation_exprs.append(
                    F.when(
                        ~F.col(field_name).isNull() & 
                        F.col(field_name).rlike('^(\\d{2}-\\d{2}-\\d{4}|\\d{4}-\\d{2}-\\d{2})$') &
                        ~F.to_date(F.col(field_name), "MM-dd-yyyy").isNotNull() &
                        ~F.to_date(F.col(field_name), "yyyy-MM-dd").isNotNull(),
                        F.lit(f"invalid date values in {field_name}")
                    )
                )
            
            # Unsigned integer validations
            elif field_type == "UInt8":
                validation_exprs.append(
                    F.when(
                        ~F.col(field_name).isNull() & 
                        ((F.col(field_name).cast("double") < 0) | 
                         (F.col(field_name).cast("double") > 255)),
                        F.lit(f"outside UInt8 range in {field_name}")
                    )
                )
            elif field_type == "UInt16":
                validation_exprs.append(
                    F.when(
                        ~F.col(field_name).isNull() & 
                        ((F.col(field_name).cast("double") < 0) | 
                         (F.col(field_name).cast("double") > 65535)),
                        F.lit(f"outside UInt16 range in {field_name}")
                    )
                )
            
            # Floating point validations
            elif isinstance(spark_type, (FloatType, DoubleType)):
                max_val = float("1.8e+308") if isinstance(spark_type, DoubleType) else float("3.4e+38")
                validation_exprs.append(
                    F.when(
                        ~F.col(field_name).isNull() & 
                        (F.abs(F.col(field_name).cast("double")) > max_val),
                        F.lit(f"invalid floating-point values in {field_name}")
                    )
                )
            
            # Decimal validations
            elif isinstance(spark_type, DecimalType):
                precision = spark_type.precision
                scale = spark_type.scale
                # Check if string matches decimal format:
                # - Integers without decimal point
                # - Numbers with decimal point and 0 to scale digits after
                decimal_regex = f"^-?\\d{{1,{precision-scale}}}(\\.\\d{{0,{scale}}})?$"
                validation_exprs.append(
                    F.when(
                        ~F.col(field_name).isNull() & 
                        ~F.col(field_name).rlike(decimal_regex),
                        F.lit(f"invalid decimal value in {field_name}")
                    )
                )
            
            # Nullable constraint
            if not nullable:
                if validation_table == "both":
                    validation_exprs.append(
                        F.when(
                            (F.col(field_name).isNull()),
                            F.lit(f"null values in non-nullable field {field_name}")
                        )
                    )
                else:
                    validation_exprs.append(
                        F.when(
                            (F.col("_validation_type") == validation_table) &
                            (F.col(field_name).isNull()),
                            F.lit(f"null values in non-nullable field {field_name}")
                        )
                    )
        
        # Check for validation errors
        if validation_exprs:
            # Create a struct with all validation expressions to collect them in parallel
            error_struct = F.struct(*[
                expr.alias(f"error_{i}") 
                for i, expr in enumerate(validation_exprs)
            ])
            error_df = df.select(error_struct)
            
            # Collect all non-null error messages from all rows
            errors = set()  # Use set to avoid duplicates
            for row in error_df.collect():
                errors.update(value for value in row[0].asDict().values() if value is not None)
            
            if errors:
                raise ValueError(f"Multiple validation errors found: {', '.join(sorted(errors))}")
        
        # Second pass: Apply transformations only to fields that exist
        for field_name, field_spec in field_specs.items():
            if field_name not in columns_to_validate:
                continue
            spec = field_spec["schema"]
            field_type = spec["type"]
            spark_type = self._get_spark_type(field_type)
            
            if isinstance(spark_type, BooleanType):
                df = df.withColumn(field_name, 
                    F.when(F.lower(F.col(field_name)).isin(["true", "1", "yes", "t", "y"]), F.lit(True))
                    .when(F.lower(F.col(field_name)).isin(["false", "0", "no", "f", "n"]), F.lit(False))
                    .otherwise(None))
            
            elif isinstance(spark_type, DateType):
                df = df.withColumn(field_name,
                    F.coalesce(
                        F.to_date(F.col(field_name), "MM-dd-yyyy"),
                        F.to_date(F.col(field_name), "yyyy-MM-dd")
                    ))
            
            else:
                df = df.withColumn(field_name, F.col(field_name).cast(spark_type))
        
        logger.info("DataFrame validation completed successfully")
        return df
    
    def _get_decimal_type(self, precision, scale):
        """Get a cached DecimalType instance or create a new one."""
        key = (precision, scale)
        if key not in self._decimal_type_cache:
            self._decimal_type_cache[key] = DecimalType(precision, scale)
        return self._decimal_type_cache[key]
    
    def _get_spark_type(self, type_spec):
        """Convert type specification to Spark type."""
        
        # Handle decimal types
        decimal_match = re.match(r"^Decimal\((\d+),(\d+)\)$", type_spec)
        if decimal_match:
            precision, scale = map(int, decimal_match.groups())
            return self._get_decimal_type(precision, scale)
            
        # Handle type strings that include parentheses (e.g., "StringType()")
        type_name = type_spec.split("(")[0] if "(" in type_spec else type_spec
        
        if type_name in self.type_mapping:
            return self.type_mapping[type_name]
            
        logger.error(f"Unsupported type specification: {type_spec}")
        raise ValueError(f"Unsupported type specification: {type_spec}")

    def get_common_field_types(self):
        """Get common field types across all schemas."""
        common_fields = {}
        
        # Combine fields from both schemas
        for schema_type in ["loan", "property"]:
            schema = self.loan_schema if schema_type == "loan" else self.property_schema
            for field_name, field_spec in schema["columns"].items():
                if field_name not in common_fields:
                    common_fields[field_name] = self._get_spark_type(field_spec["type"])
                
        return common_fields

    def _get_next_version(self, schema_type):
        """Get the next version number for a schema type."""
        try:
            current_schema = self.loan_schema if schema_type == "loan" else self.property_schema
            return current_schema.get("version", 0) + 1
        except Exception:
            return 1