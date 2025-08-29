"""
Interactive UI module for the AI assistant
"""

class InteractiveUI:
    def __init__(self, ai_assistant):
        """
        Initialize the interactive UI
        
        Args:
            ai_assistant: FunctionalProfileVanna instance
        """
        self.ai_assistant = ai_assistant
    
    def run(self):
        """Start interactive question-answering mode"""
        print("=== Functional Profile Database AI Assistant ===")
        print("Ask questions about your functional profiles, taxonomic data, and signatures!")
        print("Type 'quit' to exit, 'help' for examples, 'schema' to see database structure, or 'retrain' to update AI model.\n")
        
        while True:
            try:
                question = input("Question: ").strip()
                
                if question.lower() in ['quit', 'exit']:
                    print("Goodbye!")
                    break
                
                if question.lower() == 'help':
                    self._show_examples()
                    continue
                
                if question.lower() == 'retrain':
                    print("🔄 Retraining AI model with latest data...")
                    try:
                        self.ai_assistant.setup_training_data(force_retrain=True)
                        print("✅ Retraining complete!")
                    except Exception as e:
                        print(f"❌ Retraining failed: {e}")
                    continue
                
                if not question:
                    continue
                
                print("\n🔍 Processing your question...")
                try:
                    result = self.ai_assistant.ask_question(question)
                    
                    if result['success']:
                        print(f"\n📝 Generated SQL:")
                        print(f"```sql\n{result['sql']}\n```")
                        
                        print(f"\n📊 Results:")
                        if len(result['results']) > 0:
                            display_df = result['results'].head(20)
                            print(display_df.to_string(index=False))
                            if len(result['results']) > 20:
                                print(f"\n... and {len(result['results']) - 20} more rows")
                        else:
                            print("No results found.")
                        
                        # Generate AI response based on the results
                        print("\n🤖 AI Analysis:")
                        ai_response = self._generate_ai_response(question, result['results'])
                        print(ai_response)
                        
                        if 'explanation' in result and result['explanation']:
                            print(f"\n💡 Technical Explanation:")
                            print(result['explanation'])
                    else:
                        print(f"\n❌ Error: {result['error']}")
                        print("💡 Try rephrasing your question or use 'help' for examples.")
                    
                    print("\n" + "="*80 + "\n")
                    
                except Exception as e:
                    print(f"\n❌ Error processing question: {str(e)}")
                    print("💡 Try rephrasing your question or use 'help' for examples.")
                    print("\n" + "="*80 + "\n")
                
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                print("Please try again or type 'quit' to exit.")
    
    def _show_examples(self):
        """Show example questions"""
        examples = [
            "How many samples are in the database?",
            "Show me all available sample IDs in functional profiles",
            "What are the top 10 most abundant KO functions in sample DRR012227?",
            "Find samples containing Escherichia coli in gather results",
            "Show me taxonomic composition for sample DRR012227",
            "Which samples have the highest sequence similarity (max_containment > 0.8)?",
            "Compare protein vs DNA signature statistics",
            "Find organisms with high confidence scores in taxa profiles",
            "Show me samples with specific KO function ko:K01601",
            "What are the most common k-mer sizes used in signatures?",
            "Get gather results for samples with ANI > 95%",
            "Show me signature hash count distribution across samples",
            "Find samples with Bacteroides in their taxonomic profiles",
            "What is the average number of organisms detected per sample?",
            "Show me functional diversity (number of KO functions) per sample",
            "Get samples with the most diverse microbial communities",
            "Find samples with low sequence coverage (f_query_match < 0.1)",
            "Show me which molecule types are most common in signatures",
            "Compare abundance patterns between samples",
            "Get statistics about signature processing quality"
        ]
        
        print("\n📋 Example questions you can ask:")
        print("🧬 Functional Analysis:")
        print(f"   • {examples[0]}")
        print(f"   • {examples[1]}")
        print(f"   • {examples[2]}")
        print(f"   • {examples[8]}")
        print(f"   • {examples[14]}")
        
        print("\n🦠 Taxonomic Analysis:")
        print(f"   • {examples[4]}")
        print(f"   • {examples[7]}")
        print(f"   • {examples[12]}")
        print(f"   • {examples[13]}")
        print(f"   • {examples[15]}")
        
        print("\n🔍 Sequence Similarity:")
        print(f"   • {examples[3]}")
        print(f"   • {examples[5]}")
        print(f"   • {examples[10]}")
        print(f"   • {examples[16]}")
        
        print("\n📊 Signature Analysis:")
        print(f"   • {examples[6]}")
        print(f"   • {examples[9]}")
        print(f"   • {examples[11]}")
        print(f"   • {examples[17]}")
        print(f"   • {examples[19]}")
        
        print("\n💡 Tips:")
        print("   • Use specific sample IDs like DRR012227")
        print("   • Mention KO functions like K00001, K00002")
        print("   • Ask about organism names like 'Escherichia coli' or 'Bacteroides'")
        print("   • Query containment values, ANI scores, or confidence thresholds")
        print("   • Compare between samples or aggregate across all data")
        print()
    
    def _generate_ai_response(self, question, results):
        """
        Generate AI response based on the question, SQL query, and results
        
        Args:
            question: Original user question
            sql_query: Generated SQL query
            results: DataFrame with query results
            
        Returns:
            str: AI-generated response explaining the results
        """
        try:
            # Check if we have results
            if results is None or len(results) == 0:
                return "No data was found matching your query. This could mean:\n" \
                       "• The specific samples/organisms you're looking for aren't in the database\n" \
                       "• The criteria you specified are too restrictive\n" \
                       "• There might be a data availability issue\n" \
                       "Try broadening your search criteria or check available sample IDs."
            
            # Analyze the results based on the type of question
            response_parts = []
            
            # Basic statistics about results
            row_count = len(results)
            
            if row_count == 1:
                response_parts.append(f"Found 1 record matching your criteria.")
            else:
                response_parts.append(f"Found {row_count} records matching your criteria.")
            
            # Analyze based on column types and content
            if 'sample_id' in results.columns:
                unique_samples = results['sample_id'].nunique()
                if unique_samples > 1:
                    response_parts.append(f"Data spans across {unique_samples} different samples.")
                elif unique_samples == 1:
                    sample_name = results['sample_id'].iloc[0]
                    response_parts.append(f"All data is from sample: {sample_name}")
            
            # Analyze numerical columns for insights
            numerical_cols = results.select_dtypes(include=['float64', 'int64']).columns
            for col in numerical_cols:
                if col in ['abundance', 'jaccard', 'max_containment', 'f_query_match', 'scale_factor']:
                    col_data = results[col].dropna()
                    if len(col_data) > 0:
                        if col == 'abundance':
                            total_abundance = col_data.sum()
                            max_abundance = col_data.max()
                            response_parts.append(f"Total abundance: {total_abundance:.3f}, highest single value: {max_abundance:.3f}")
                        elif col in ['jaccard', 'max_containment', 'f_query_match']:
                            avg_val = col_data.mean()
                            response_parts.append(f"Average {col}: {avg_val:.3f} (ranges from {col_data.min():.3f} to {col_data.max():.3f})")
            
            # Identify top results if applicable
            if 'ko_id' in results.columns and 'abundance' in results.columns:
                top_ko = results.nlargest(3, 'abundance')
                if len(top_ko) > 0:
                    response_parts.append("Top functional annotations:")
                    for _, row in top_ko.iterrows():
                        response_parts.append(f"  • {row['ko_id']}: {row['abundance']:.3f}")
            
            if 'organism_name' in results.columns:
                if 'actual_confidence_with_coverage' in results.columns:
                    high_conf = results[results['actual_confidence_with_coverage'] > 0.8]
                    if len(high_conf) > 0:
                        response_parts.append(f"High-confidence organism detections ({len(high_conf)} found):")
                        for _, row in high_conf.head(3).iterrows():
                            org_name = row['organism_name'][:50] + "..." if len(str(row['organism_name'])) > 50 else row['organism_name']
                            response_parts.append(f"  • {org_name} (confidence: {row['actual_confidence_with_coverage']:.3f})")
            
            # Context-specific insights based on question keywords
            question_lower = question.lower()
            if 'diversity' in question_lower or 'variety' in question_lower:
                if len(results) > 10:
                    response_parts.append("This shows high diversity in your sample.")
                elif len(results) < 3:
                    response_parts.append("This indicates relatively low diversity.")
            
            if 'comparison' in question_lower or 'compare' in question_lower:
                if 'sample_id' in results.columns and results['sample_id'].nunique() > 1:
                    response_parts.append("The comparison shows differences between samples - examine the numerical values to identify patterns.")
            
            if 'statistics' in question_lower or 'stats' in question_lower:
                response_parts.append("Key statistics are displayed above. Look for patterns in the numerical distributions.")
            
            # Add interpretation based on data type
            if any(col in results.columns for col in ['md5', 'min_hash', 'signature_size']):
                response_parts.append("This signature data can help identify sequence similarity and genomic relationships.")
            
            if any(col in results.columns for col in ['country', 'biome', 'lat_lon']):
                response_parts.append("Geographic data is included - this could be useful for spatial analysis of your samples.")
            
            # Final recommendation
            if row_count > 20:
                response_parts.append(f"\n💡 Tip: Only showing first 20 rows. Use more specific filters to narrow down the {row_count} total results.")
            
            return "\n".join(response_parts)
            
        except Exception as e:
            return f"Could not generate detailed analysis: {str(e)}\nThe raw results are displayed above."
