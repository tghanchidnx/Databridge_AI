"""
Formula Engine for DataBridge AI Librarian.

Handles hierarchy calculations including SUM, SUBTRACT, MULTIPLY, DIVIDE, and PERCENT.
"""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, List, Dict, Any, Union, Callable


class FormulaOperation(str, Enum):
    """Supported formula operations."""

    SUM = "SUM"
    SUBTRACT = "SUBTRACT"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    PERCENT = "PERCENT"
    AVERAGE = "AVERAGE"
    MIN = "MIN"
    MAX = "MAX"
    COUNT = "COUNT"
    ABS = "ABS"
    ROUND = "ROUND"


@dataclass
class FormulaResult:
    """Result of a formula calculation."""

    success: bool
    value: Optional[Decimal]
    formula: str
    sources: List[str]
    operation: str
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "value": float(self.value) if self.value is not None else None,
            "formula": self.formula,
            "sources": self.sources,
            "operation": self.operation,
            "error": self.error,
        }


@dataclass
class FormulaRuleConfig:
    """Configuration for a formula rule."""

    target_hierarchy_id: str
    source_hierarchy_ids: List[str]
    operation: FormulaOperation
    rule_order: int = 0
    round_decimals: Optional[int] = None
    null_handling: str = "zero"  # zero, skip, error


class FormulaEngine:
    """
    Engine for calculating hierarchy formulas.

    Supports:
    - Basic arithmetic (SUM, SUBTRACT, MULTIPLY, DIVIDE)
    - Percentage calculations
    - Aggregations (AVERAGE, MIN, MAX, COUNT)
    - Custom formula functions
    """

    def __init__(self, decimal_places: int = 2):
        """
        Initialize the formula engine.

        Args:
            decimal_places: Default decimal places for rounding.
        """
        self.decimal_places = decimal_places
        self._custom_functions: Dict[str, Callable] = {}

    def register_function(
        self,
        name: str,
        func: Callable[[List[Decimal]], Decimal],
    ) -> None:
        """
        Register a custom formula function.

        Args:
            name: Function name.
            func: Function that takes list of Decimal values and returns Decimal.
        """
        self._custom_functions[name.upper()] = func

    def calculate(
        self,
        operation: Union[FormulaOperation, str],
        values: List[Union[Decimal, float, int, None]],
        round_decimals: Optional[int] = None,
        null_handling: str = "zero",
    ) -> FormulaResult:
        """
        Perform a calculation on a list of values.

        Args:
            operation: The operation to perform.
            values: List of numeric values.
            round_decimals: Optional decimal places for rounding.
            null_handling: How to handle None values ('zero', 'skip', 'error').

        Returns:
            FormulaResult with the calculation result.
        """
        if isinstance(operation, str):
            try:
                operation = FormulaOperation(operation.upper())
            except ValueError:
                # Check custom functions
                if operation.upper() in self._custom_functions:
                    return self._calculate_custom(operation.upper(), values, round_decimals, null_handling)
                return FormulaResult(
                    success=False,
                    value=None,
                    formula=f"{operation}({len(values)} values)",
                    sources=[],
                    operation=operation,
                    error=f"Unknown operation: {operation}",
                )

        # Handle null values
        processed_values: List[Decimal] = []
        for v in values:
            if v is None:
                if null_handling == "error":
                    return FormulaResult(
                        success=False,
                        value=None,
                        formula=f"{operation.value}({len(values)} values)",
                        sources=[],
                        operation=operation.value,
                        error="Null value encountered",
                    )
                elif null_handling == "zero":
                    processed_values.append(Decimal("0"))
                # 'skip' - don't add to list
            else:
                try:
                    processed_values.append(Decimal(str(v)))
                except InvalidOperation:
                    return FormulaResult(
                        success=False,
                        value=None,
                        formula=f"{operation.value}({len(values)} values)",
                        sources=[],
                        operation=operation.value,
                        error=f"Invalid numeric value: {v}",
                    )

        # Perform calculation
        try:
            result = self._execute_operation(operation, processed_values)
        except Exception as e:
            return FormulaResult(
                success=False,
                value=None,
                formula=f"{operation.value}({len(processed_values)} values)",
                sources=[],
                operation=operation.value,
                error=str(e),
            )

        # Round if specified
        if result is not None:
            decimals = round_decimals if round_decimals is not None else self.decimal_places
            result = round(result, decimals)

        return FormulaResult(
            success=True,
            value=result,
            formula=f"{operation.value}({len(processed_values)} values)",
            sources=[],
            operation=operation.value,
        )

    def _execute_operation(
        self,
        operation: FormulaOperation,
        values: List[Decimal],
    ) -> Optional[Decimal]:
        """Execute a formula operation."""
        if not values:
            return Decimal("0")

        if operation == FormulaOperation.SUM:
            return sum(values)

        elif operation == FormulaOperation.SUBTRACT:
            if len(values) < 1:
                return Decimal("0")
            result = values[0]
            for v in values[1:]:
                result -= v
            return result

        elif operation == FormulaOperation.MULTIPLY:
            result = Decimal("1")
            for v in values:
                result *= v
            return result

        elif operation == FormulaOperation.DIVIDE:
            if len(values) < 2:
                return values[0] if values else Decimal("0")
            result = values[0]
            for v in values[1:]:
                if v == 0:
                    raise ValueError("Division by zero")
                result /= v
            return result

        elif operation == FormulaOperation.PERCENT:
            if len(values) < 2:
                return Decimal("0")
            numerator = values[0]
            denominator = values[1]
            if denominator == 0:
                return Decimal("0")
            return (numerator / denominator) * Decimal("100")

        elif operation == FormulaOperation.AVERAGE:
            if not values:
                return Decimal("0")
            return sum(values) / Decimal(len(values))

        elif operation == FormulaOperation.MIN:
            return min(values) if values else Decimal("0")

        elif operation == FormulaOperation.MAX:
            return max(values) if values else Decimal("0")

        elif operation == FormulaOperation.COUNT:
            return Decimal(len(values))

        elif operation == FormulaOperation.ABS:
            if not values:
                return Decimal("0")
            return abs(values[0])

        elif operation == FormulaOperation.ROUND:
            if not values:
                return Decimal("0")
            return round(values[0], 0)

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def _calculate_custom(
        self,
        func_name: str,
        values: List[Union[Decimal, float, int, None]],
        round_decimals: Optional[int],
        null_handling: str,
    ) -> FormulaResult:
        """Execute a custom formula function."""
        func = self._custom_functions.get(func_name)
        if not func:
            return FormulaResult(
                success=False,
                value=None,
                formula=f"{func_name}({len(values)} values)",
                sources=[],
                operation=func_name,
                error=f"Unknown function: {func_name}",
            )

        # Handle null values
        processed_values: List[Decimal] = []
        for v in values:
            if v is None:
                if null_handling == "error":
                    return FormulaResult(
                        success=False,
                        value=None,
                        formula=f"{func_name}({len(values)} values)",
                        sources=[],
                        operation=func_name,
                        error="Null value encountered",
                    )
                elif null_handling == "zero":
                    processed_values.append(Decimal("0"))
            else:
                processed_values.append(Decimal(str(v)))

        try:
            result = func(processed_values)
            if round_decimals is not None:
                result = round(result, round_decimals)
            return FormulaResult(
                success=True,
                value=result,
                formula=f"{func_name}({len(processed_values)} values)",
                sources=[],
                operation=func_name,
            )
        except Exception as e:
            return FormulaResult(
                success=False,
                value=None,
                formula=f"{func_name}({len(processed_values)} values)",
                sources=[],
                operation=func_name,
                error=str(e),
            )

    def calculate_hierarchy_values(
        self,
        rule: FormulaRuleConfig,
        values: Dict[str, Union[Decimal, float, int, None]],
    ) -> FormulaResult:
        """
        Calculate a hierarchy value based on a formula rule.

        Args:
            rule: Formula rule configuration.
            values: Dictionary mapping hierarchy_id to its value.

        Returns:
            FormulaResult with the calculation result.
        """
        # Get source values in order
        source_values = []
        missing_sources = []

        for source_id in rule.source_hierarchy_ids:
            if source_id in values:
                source_values.append(values[source_id])
            else:
                missing_sources.append(source_id)

        if missing_sources and rule.null_handling == "error":
            return FormulaResult(
                success=False,
                value=None,
                formula=f"{rule.operation.value}({rule.source_hierarchy_ids})",
                sources=rule.source_hierarchy_ids,
                operation=rule.operation.value,
                error=f"Missing source values: {missing_sources}",
            )

        result = self.calculate(
            operation=rule.operation,
            values=source_values,
            round_decimals=rule.round_decimals,
            null_handling=rule.null_handling,
        )

        # Add source information
        result.sources = rule.source_hierarchy_ids
        result.formula = f"{rule.operation.value}({', '.join(rule.source_hierarchy_ids)}) -> {rule.target_hierarchy_id}"

        return result

    def calculate_tree_rollup(
        self,
        tree_values: Dict[str, Union[Decimal, float, int, None]],
        parent_child_map: Dict[str, List[str]],
        operation: FormulaOperation = FormulaOperation.SUM,
    ) -> Dict[str, FormulaResult]:
        """
        Calculate rolled-up values for a hierarchy tree.

        Args:
            tree_values: Dictionary mapping hierarchy_id to its direct value.
            parent_child_map: Dictionary mapping parent_id to list of child_ids.
            operation: Aggregation operation for rollup (default: SUM).

        Returns:
            Dictionary mapping hierarchy_id to FormulaResult.
        """
        results: Dict[str, FormulaResult] = {}

        # Process from leaves to roots (topological sort)
        # First, find all nodes with no children (leaves)
        all_children = set()
        for children in parent_child_map.values():
            all_children.update(children)

        all_parents = set(parent_child_map.keys())
        all_nodes = all_parents | all_children

        # Process nodes - leaves first, then parents
        processed = set()

        def process_node(node_id: str) -> Decimal:
            if node_id in processed:
                return results[node_id].value or Decimal("0")

            children = parent_child_map.get(node_id, [])

            if not children:
                # Leaf node - use direct value
                direct_value = tree_values.get(node_id)
                if direct_value is None:
                    direct_value = Decimal("0")
                else:
                    direct_value = Decimal(str(direct_value))

                results[node_id] = FormulaResult(
                    success=True,
                    value=direct_value,
                    formula=f"LEAF({node_id})",
                    sources=[],
                    operation="LEAF",
                )
                processed.add(node_id)
                return direct_value

            # Process children first
            child_values = []
            for child_id in children:
                child_value = process_node(child_id)
                child_values.append(child_value)

            # Calculate parent value
            result = self.calculate(operation, child_values)
            result.sources = children
            result.formula = f"{operation.value}({', '.join(children)}) -> {node_id}"

            results[node_id] = result
            processed.add(node_id)
            return result.value or Decimal("0")

        # Process all root nodes (not in any children list)
        root_nodes = all_parents - all_children
        for root_id in root_nodes:
            process_node(root_id)

        # Process any remaining nodes
        for node_id in all_nodes:
            if node_id not in processed:
                process_node(node_id)

        return results

    def evaluate_expression(
        self,
        expression: str,
        values: Dict[str, Union[Decimal, float, int, None]],
    ) -> FormulaResult:
        """
        Evaluate a simple formula expression.

        Supports expressions like:
        - "SUM(A, B, C)"
        - "A + B - C"
        - "A * B / C"
        - "(A + B) * C"

        Args:
            expression: Formula expression.
            values: Dictionary mapping variable names to values.

        Returns:
            FormulaResult with the evaluation result.
        """
        # Handle function-style expressions
        expression = expression.strip()

        # Check for function call format
        for op in FormulaOperation:
            if expression.upper().startswith(f"{op.value}("):
                # Extract arguments
                args_str = expression[len(op.value) + 1:-1]
                args = [a.strip() for a in args_str.split(",")]

                # Resolve values
                resolved_values = []
                for arg in args:
                    if arg in values:
                        resolved_values.append(values[arg])
                    else:
                        try:
                            resolved_values.append(Decimal(arg))
                        except InvalidOperation:
                            return FormulaResult(
                                success=False,
                                value=None,
                                formula=expression,
                                sources=args,
                                operation=op.value,
                                error=f"Unknown variable or invalid number: {arg}",
                            )

                result = self.calculate(op, resolved_values)
                result.formula = expression
                result.sources = args
                return result

        # Handle simple arithmetic expressions
        # This is a basic implementation - for complex expressions, consider using
        # a proper expression parser
        try:
            # Replace variable names with values
            eval_expr = expression
            used_vars = []
            for var_name, var_value in sorted(values.items(), key=lambda x: -len(x[0])):
                if var_name in eval_expr:
                    used_vars.append(var_name)
                    if var_value is None:
                        eval_expr = eval_expr.replace(var_name, "0")
                    else:
                        eval_expr = eval_expr.replace(var_name, str(var_value))

            # Safely evaluate (only allow basic arithmetic)
            allowed_chars = set("0123456789.+-*/() ")
            if not all(c in allowed_chars for c in eval_expr):
                return FormulaResult(
                    success=False,
                    value=None,
                    formula=expression,
                    sources=used_vars,
                    operation="EXPRESSION",
                    error="Invalid characters in expression",
                )

            result = eval(eval_expr)  # Safe due to character filtering
            return FormulaResult(
                success=True,
                value=Decimal(str(result)),
                formula=expression,
                sources=used_vars,
                operation="EXPRESSION",
            )
        except Exception as e:
            return FormulaResult(
                success=False,
                value=None,
                formula=expression,
                sources=[],
                operation="EXPRESSION",
                error=str(e),
            )
