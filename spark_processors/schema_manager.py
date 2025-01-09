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
        self.loan_schema = self._load_current_schema("loan", "loan_schema.json")
        self.property_schema = self._load_current_schema("property", "property_schema.json")
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
                base_schema["version"] = 1
                base_schema["effective_date"] = datetime.now().isoformat()
            
            self._save_schema(schema_type, base_schema)
            logger.info(f"Created initial schema version for {schema_type}")
            return base_schema
            
        latest_schema_file = schema_files[-1]
        logger.info(f"Loading schema from {latest_schema_file}")
        with open(os.path.join(schema_dir, latest_schema_file), 'r') as f:
            return json.load(f)
    
    def _save_schema(self, schema_type, schema=None):
        """Save schema to file."""
        try:
            schema = schema or (self.loan_schema if schema_type == "loan" else self.property_schema)
            version = schema["version"]
            
            # Add metadata
            schema["effective_date"] = datetime.now().isoformat()
            
            # Save to file
            filepath = os.path.join(
                self.schema_dir,
                f"{schema_type}_schema_v{version}.json"
            )
            
            with open(filepath, "w") as f:
                json.dump(schema, f, indent=2)
                
            logger.info(f"Saving schema version {version} for {schema_type}")
            logger.info(f"Schema saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving schema: {str(e)}")
            raise
    
    def update_schema(self, schema_type="loan", field_specs=None):
        """Update schema with new field specifications.
        
        Args:
            schema_type: Type of schema to update ('loan' or 'property')
            field_specs: Dictionary of field specifications to update
        """
        logger.info(f"Updating {schema_type} schema")
        
        # Create a new schema with the provided field specs
        new_schema = {
            "version": self._get_next_version(schema_type),
            "columns": field_specs or {}
        }
        
        # Replace the existing schema
        if schema_type == "loan":
            self.loan_schema = new_schema
        else:
            self.property_schema = new_schema
            
        # Save updated schema
        self._save_schema(schema_type)
        
        return self.get_spark_schema(schema_type)
    
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
        """
        logger.info("Validating DataFrame against schema")

        try:
            # Convert single string to list for uniform handling
            if isinstance(validation_configs, str):
                validation_configs = [validation_configs]
            elif not isinstance(validation_configs, list):
                raise ValueError("validation_configs must be a string schema type or list of schema types")

            # Get all field specs to validate
            all_field_specs = []
            for config in validation_configs:
                if not isinstance(config, str) or config not in ["loan", "property"]:
                    raise ValueError("Each validation config must be either 'loan' or 'property'")
                schema = self.loan_schema if config == "loan" else self.property_schema
                for col in schema["columns"]:
                    all_field_specs.append({
                    "column": col,
                    "table": config,
                    "schema": schema["columns"][col]
                })

            return self._validate_field_specs(df, all_field_specs)

        except Exception as e:
            logger.error(f"Error during DataFrame validation: {str(e)}")
            raise
    
    def _validate_field_specs(self, df, field_specs):
        """Validate DataFrame against field specifications."""
        # Check for columns in DataFrame that aren't in the schema
        schema_columns = set([field["column"] for field in field_specs])
        unknown_columns = {col for col in df.columns if col != "_validation_type" and col not in schema_columns}
        if unknown_columns:
            raise ValueError(f"Found columns in DataFrame that are not defined in schema: {unknown_columns}")
        
        # Get the intersection of DataFrame columns and field specs
        columns_to_validate = set(df.columns).intersection(schema_columns)
        
        # First pass: Validate all fields that exist in the DataFrame
        validation_exprs = []
        
        for field_spec in field_specs:
            field_name = field_spec["column"]
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
        for field_spec in field_specs:
            field_name = field_spec["column"]
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