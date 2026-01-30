"""
Unit tests for FormulaEngine.
"""

import pytest
from decimal import Decimal


class TestBasicOperations:
    """Tests for basic formula operations."""

    def test_sum_operation(self):
        """Test SUM operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.SUM, [10, 20, 30])

        assert result.success is True
        assert result.value == Decimal("60")

    def test_subtract_operation(self):
        """Test SUBTRACT operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.SUBTRACT, [100, 30, 20])

        assert result.success is True
        assert result.value == Decimal("50")

    def test_multiply_operation(self):
        """Test MULTIPLY operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.MULTIPLY, [5, 4, 3])

        assert result.success is True
        assert result.value == Decimal("60")

    def test_divide_operation(self):
        """Test DIVIDE operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.DIVIDE, [100, 4, 5])

        assert result.success is True
        assert result.value == Decimal("5")

    def test_divide_by_zero(self):
        """Test division by zero handling."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.DIVIDE, [100, 0])

        assert result.success is False
        assert "zero" in result.error.lower()

    def test_percent_operation(self):
        """Test PERCENT operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.PERCENT, [25, 100])

        assert result.success is True
        assert result.value == Decimal("25")

    def test_average_operation(self):
        """Test AVERAGE operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.AVERAGE, [10, 20, 30])

        assert result.success is True
        assert result.value == Decimal("20")

    def test_min_operation(self):
        """Test MIN operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.MIN, [30, 10, 20])

        assert result.success is True
        assert result.value == Decimal("10")

    def test_max_operation(self):
        """Test MAX operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.MAX, [30, 10, 20])

        assert result.success is True
        assert result.value == Decimal("30")

    def test_count_operation(self):
        """Test COUNT operation."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.COUNT, [1, 2, 3, 4, 5])

        assert result.success is True
        assert result.value == Decimal("5")


class TestNullHandling:
    """Tests for null value handling."""

    def test_null_as_zero(self):
        """Test handling nulls as zero."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(
            FormulaOperation.SUM,
            [10, None, 20],
            null_handling="zero",
        )

        assert result.success is True
        assert result.value == Decimal("30")

    def test_null_skip(self):
        """Test skipping null values."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(
            FormulaOperation.SUM,
            [10, None, 20],
            null_handling="skip",
        )

        assert result.success is True
        assert result.value == Decimal("30")

    def test_null_error(self):
        """Test error on null values."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(
            FormulaOperation.SUM,
            [10, None, 20],
            null_handling="error",
        )

        assert result.success is False
        assert "null" in result.error.lower()


class TestRounding:
    """Tests for rounding."""

    def test_default_rounding(self):
        """Test default rounding to 2 decimal places."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine(decimal_places=2)
        result = engine.calculate(FormulaOperation.DIVIDE, [100, 3])

        assert result.success is True
        assert result.value == Decimal("33.33")

    def test_custom_rounding(self):
        """Test custom rounding."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(
            FormulaOperation.DIVIDE,
            [100, 3],
            round_decimals=4,
        )

        assert result.success is True
        assert result.value == Decimal("33.3333")


class TestStringOperation:
    """Tests for string-based operation names."""

    def test_string_operation_name(self):
        """Test using string operation name."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        result = engine.calculate("SUM", [10, 20, 30])

        assert result.success is True
        assert result.value == Decimal("60")

    def test_lowercase_operation_name(self):
        """Test using lowercase operation name."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        result = engine.calculate("sum", [10, 20])

        assert result.success is True
        assert result.value == Decimal("30")

    def test_unknown_operation(self):
        """Test unknown operation name."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        result = engine.calculate("UNKNOWN_OP", [10, 20])

        assert result.success is False
        assert "unknown" in result.error.lower()


class TestFormulaResult:
    """Tests for FormulaResult."""

    def test_to_dict(self):
        """Test converting result to dictionary."""
        from src.hierarchy.formula import FormulaEngine, FormulaOperation

        engine = FormulaEngine()
        result = engine.calculate(FormulaOperation.SUM, [10, 20])

        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["value"] == 30.0
        assert result_dict["operation"] == "SUM"


class TestHierarchyCalculations:
    """Tests for hierarchy-specific calculations."""

    def test_calculate_hierarchy_values(self):
        """Test calculating hierarchy values."""
        from src.hierarchy.formula import FormulaEngine, FormulaRuleConfig, FormulaOperation

        engine = FormulaEngine()
        rule = FormulaRuleConfig(
            target_hierarchy_id="TOTAL",
            source_hierarchy_ids=["A", "B", "C"],
            operation=FormulaOperation.SUM,
        )
        values = {"A": 100, "B": 200, "C": 300}

        result = engine.calculate_hierarchy_values(rule, values)

        assert result.success is True
        assert result.value == Decimal("600")
        assert result.sources == ["A", "B", "C"]

    def test_calculate_tree_rollup(self):
        """Test tree rollup calculation."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()

        # Simple tree: ROOT -> (A, B), A -> (A1, A2)
        tree_values = {
            "A1": 100,
            "A2": 200,
            "B": 300,
        }
        parent_child_map = {
            "ROOT": ["A", "B"],
            "A": ["A1", "A2"],
        }

        results = engine.calculate_tree_rollup(tree_values, parent_child_map)

        assert results["A1"].value == Decimal("100")
        assert results["A2"].value == Decimal("200")
        assert results["A"].value == Decimal("300")  # A1 + A2
        assert results["B"].value == Decimal("300")
        assert results["ROOT"].value == Decimal("600")  # A + B


class TestExpressionEvaluation:
    """Tests for expression evaluation."""

    def test_function_expression(self):
        """Test evaluating function-style expression."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        values = {"A": 100, "B": 200}

        result = engine.evaluate_expression("SUM(A, B)", values)

        assert result.success is True
        assert result.value == Decimal("300")

    def test_arithmetic_expression(self):
        """Test evaluating arithmetic expression."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        values = {"A": 100, "B": 50}

        result = engine.evaluate_expression("A + B", values)

        assert result.success is True
        assert result.value == Decimal("150")

    def test_complex_arithmetic(self):
        """Test complex arithmetic expression."""
        from src.hierarchy.formula import FormulaEngine

        engine = FormulaEngine()
        values = {"A": 100, "B": 50, "C": 2}

        result = engine.evaluate_expression("(A + B) * C", values)

        assert result.success is True
        assert result.value == Decimal("300")


class TestCustomFunctions:
    """Tests for custom formula functions."""

    def test_register_custom_function(self):
        """Test registering and using a custom function."""
        from src.hierarchy.formula import FormulaEngine
        from decimal import Decimal

        engine = FormulaEngine()

        # Register a custom weighted sum function
        def weighted_sum(values):
            if len(values) < 2:
                return sum(values)
            weight = values[-1]
            return sum(v * weight for v in values[:-1])

        engine.register_function("WEIGHTED", weighted_sum)

        result = engine.calculate("WEIGHTED", [10, 20, Decimal("0.5")])

        assert result.success is True
        assert result.value == Decimal("15")  # (10 + 20) * 0.5
